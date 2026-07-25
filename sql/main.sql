-- Purpose
-- 
-- Orchestrate the DB-side ETL for OSM-derived amenities and roads into a clean, queryable model schema.
-- Read administrative areas from /data/admin.gpkg; build a unified planning area (EPSG::TARGET_SRID + 15 km buffer).
-- Generate an H3 grid (resolution 10) covering the planning area for aggregation and joins.
-- Merge raw OSM features (points + areas) imported by osm2pgsql style.lua into a single staging stream.
-- Classify features via config-driven rules (CSV and optional SQL snippets), then normalise geometries per policy.
-- Validate geometries; drop empty; de-duplicate points that fall within same-class polygons; publish model.amenities*.
-- Derive entrance candidates by intersecting polygon boundaries with roads, with fallbacks and deterministic IDs.
-- Attach H3 indices (r=10) to amenities and entrances for hex-based analytics.
-- 
-- Inputs
-- Raw OSM tables produced by style.lua: public.raw_points_of_interest, public.raw_areas_of_interest, public.raw_lines_roads.
-- /data/admin.gpkg — GeoPackage with polygon/multipolygon layer(s) including a 'name' column.
-- /sql/class_a_config.csv — class definitions and rule pointers.
-- /sql/class_a_defs/*.sql — optional full-SQL snippets per class (if referenced by the CSV).

-- Outputs (key objects)
-- admin.admin_areas, admin.planning_area, admin.grid (H3 r=10).
-- staging.unified_features_mat, staging.class_a_config, staging.rule_hits, staging.rule_hits_normalized, staging.classified_valid.
-- model.amenities_points, model.amenities_polygons, model.amenities (view), model.amenities_h3.
-- model.roads, model.entrances.
-- 
-- Key conventions
-- Geometry SRID: :TARGET_SRID for processing/buffering; 4326 only where H3/geography are required.
-- H3 resolution: 10.
-- Tag keys with ':' are normalised to '_' to match SQL column names.
-- entrance_id = 'entr_' || md5(parent_osm_id | road_osm_id | EWKB(geom)) — stable if inputs remain unchanged.

-- =========================================================
-- 0) Extensions & Schemas
--    - Ensure spatial (PostGIS), raster, and H3 extensions are present.
--    - Create a working schema for staging (temporary/derivative objects).
-- =========================================================
CREATE EXTENSION IF NOT EXISTS postgis;          -- geometry/geography types + spatial functions
CREATE EXTENSION IF NOT EXISTS postgis_raster;   -- for H3 support
CREATE EXTENSION IF NOT EXISTS h3;               -- H3 core (hexagonal indexing)
CREATE EXTENSION IF NOT EXISTS h3_postgis;       -- PostGIS bindings for H3
CREATE EXTENSION IF NOT EXISTS postgis_raster;   -- Raster support
CREATE SCHEMA IF NOT EXISTS staging;             -- scratch area for ETL before publishing to model schema

-- Clean up and (re)create helper function that converts a GEOMETRY to H3 index text
DROP FUNCTION IF EXISTS staging.h3_from_geom(geometry, integer) CASCADE;

--
-- staging.h3_from_geom(g geometry, res integer) -> text
-- Purpose: Convert an input geometry (any type) to an H3 cell at a given resolution.
-- Details:
-- Forces WGS84 (EPSG:4326) because H3 expects lat/lon coordinates.
-- Casts to point: for non-point inputs, caller should pass a representative point
--  (e.g., centroid or point-on-surface) BEFORE calling this function.
--  Uses h3_lat_lng_to_cell from h3_postgis to compute the index, returns as TEXT for convenience.
-- Characteristics: IMMUTABLE/STRICT/SAFE for planner parallelism.
CREATE OR REPLACE FUNCTION staging.h3_from_geom(g geometry, res integer)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
  -- Normalize to WGS84 point (x=lon, y=lat). Will raise if g isn't convertible to point.
  WITH gg AS (
    SELECT CASE WHEN ST_SRID(g)=4326 THEN g ELSE ST_Transform(g,4326) END
             ::geometry(Point,4326) AS p
  )
  SELECT h3_lat_lng_to_cell( 
           point(ST_X((SELECT p FROM gg)), ST_Y((SELECT p FROM gg))),
           res
         )::text
$$;

-- =========================================================
-- A) ADMIN INPUT via OGR_FDW  (expects /data/admin.gpkg)
--    - Reads any layer from the GeoPackage that has a 'name' column and a polygon/multipolygon geometry.
--    - Consolidates them into admin._in as (name, geom[4326]).
--    - Fails early with a clear error if nothing suitable is found.
-- =========================================================
CREATE SCHEMA IF NOT EXISTS admin;         -- namespace for administrative inputs/outputs
CREATE EXTENSION IF NOT EXISTS ogr_fdw;    -- foreign table wrapper for GDAL/OGR sources

-- Set up foreign server pointing at the GeoPackage
DROP SERVER IF EXISTS admin_gpkg_srv CASCADE;
CREATE SERVER admin_gpkg_srv
  FOREIGN DATA WRAPPER ogr_fdw
  OPTIONS (datasource '/data/admin.gpkg', format 'GPKG');

-- Import all GPKG layers into a dedicated schema via ogr_fdw
DROP SCHEMA IF EXISTS admin_fdw CASCADE;
CREATE SCHEMA admin_fdw;

-- Bring in exposed layers as foreign tables (names come from the GPKG)
IMPORT FOREIGN SCHEMA ogr_all
  FROM SERVER admin_gpkg_srv
  INTO admin_fdw;

-- Local consolidated input table
DROP TABLE IF EXISTS admin._in;
CREATE TABLE admin._in (
  name text,
  geom geometry(MultiPolygon,4326)
);

-- Iterate over all foreign tables to collect layers that have a 'name' column and a geometry column
DO $$
DECLARE
  r record;
  sql text;
BEGIN
  FOR r IN
    SELECT g.table_schema AS sch,
           g.table_name   AS tbl,
           g.column_name  AS gcol
    FROM information_schema.columns g
    JOIN information_schema.columns n
      ON n.table_schema = g.table_schema
     AND n.table_name   = g.table_name
     AND n.column_name  = 'name'
    WHERE g.table_schema = 'admin_fdw'
      AND g.udt_name = 'geometry'
  LOOP
    -- For each candidate foreign table: copy name + polygonal geometry into admin._in
    sql := format($f$
      INSERT INTO admin._in (name, geom)
      SELECT name::text,
             ST_Transform(ST_Multi(ST_CollectionExtract(%1$I, 3)), 4326)
      FROM %2$I.%3$I
      WHERE %1$I IS NOT NULL
    $f$, r.gcol, r.sch, r.tbl);
    EXECUTE sql;
  END LOOP;

  -- Safety: if nothing was loaded, raise a human-readable error
  IF (SELECT count(*) FROM admin._in) = 0 THEN
    RAISE EXCEPTION 'admin.gpkg contains no layer with column name and polygon/multipolygon geometry';
  END IF;
END $$;

-- =========================================================
-- B) ADMIN AREAS (SRID :TARGET_SRID), PLANNING AREA (dissolve + buffer 15 km), GRID H3 r=10
--    - Normalize and validate inputs into admin.admin_areas (EPSG::TARGET_SRID for consistent buffering/lengths).
--    - Build a single "planning_area" by dissolving and buffering by 15 km.
--    - Generate an H3 grid at resolution 10 that covers the planning area, with centroids and admin names.
-- =========================================================

-- Normalize to :TARGET_SRID and ensure polygonal validity
DROP TABLE IF EXISTS admin.admin_areas CASCADE;
CREATE TABLE admin.admin_areas AS
SELECT
  name,
  ST_Multi(ST_Transform(ST_MakeValid(geom), :TARGET_SRID))::geometry(MultiPolygon,:TARGET_SRID) AS geom
FROM admin._in
WHERE name IS NOT NULL;

-- Dissolve by name (merge multipart boundaries for the same name)
CREATE TABLE admin.__tmp AS
SELECT
  name,
  ST_Multi(ST_UnaryUnion(ST_Collect(geom)))::geometry(MultiPolygon,:TARGET_SRID) AS geom
FROM admin.admin_areas
GROUP BY name;

DROP TABLE admin.admin_areas;
ALTER TABLE admin.__tmp RENAME TO admin_areas;

ALTER TABLE admin.admin_areas ADD PRIMARY KEY (name);
CREATE INDEX IF NOT EXISTS admin_areas_geom_gix ON admin.admin_areas USING GIST (geom);

-- Build planning area: dissolve all admin areas and add a 15 km buffer (to capture near-by features)
DROP TABLE IF EXISTS admin.planning_area CASCADE;
CREATE TABLE admin.planning_area AS
SELECT
  ST_Buffer(ST_UnaryUnion(ST_Collect(geom)), :ADMIN_BUFFER)::geometry(MultiPolygon,:TARGET_SRID) AS geom
FROM admin.admin_areas;

CREATE INDEX IF NOT EXISTS planning_area_geom_gix ON admin.planning_area USING GIST (geom);

-- H3 grid table: one row per H3 cell intersecting the planning area, with centroid (in :TARGET_SRID) and admin name
DROP TABLE IF EXISTS admin.grid CASCADE;
CREATE TABLE admin.grid (
  h3_cell    text PRIMARY KEY,            -- H3 index (resolution 10) as text
  admin_name text,                        -- joined from admin.admin_areas
  geom       geometry(Point,:TARGET_SRID)         -- cell centroid in :TARGET_SRID
);

WITH pa AS (
  -- H3 expects WGS84 polygons
  SELECT ST_Transform(geom, 4326) AS g4326 FROM admin.planning_area
),
cells AS (
  -- H3 coverage (resolution 10) of the planning area polygon
  SELECT h3_polygon_to_cells(g4326, 10) AS h3_cell
  FROM pa
),
cent AS (
  -- Compute representative point: centroid of H3 cell boundary
  SELECT
    c.h3_cell::h3index AS h3i,
    ST_Centroid(h3_cell_to_boundary_geometry(c.h3_cell::h3index))::geometry(Point,4326) AS gpt4326
  FROM cells c
)
INSERT INTO admin.grid (h3_cell, admin_name, geom)
SELECT
  (h3i)::text AS h3_cell,
  a.name      AS admin_name,
  ST_Transform(gpt4326, :TARGET_SRID)::geometry(Point,:TARGET_SRID) AS geom
FROM cent
LEFT JOIN admin.admin_areas a
  ON ST_Contains(a.geom, ST_Transform(gpt4326,:TARGET_SRID));

CREATE INDEX IF NOT EXISTS admin_grid_geom_gix      ON admin.grid USING GIST (geom);
CREATE INDEX IF NOT EXISTS admin_grid_admin_name_ix ON admin.grid (admin_name);

-- =========================================================
-- 1) Unified features as MATERIALIZED VIEW (fast & indexable)
--    - Merge raw points and areas of interest into a single stream with a type label.
--    - Restrict to planning_area for performance and relevance.
-- =========================================================

DROP MATERIALIZED VIEW IF EXISTS staging.unified_features_mat CASCADE;
CREATE MATERIALIZED VIEW staging.unified_features_mat AS
WITH pa AS (SELECT geom FROM admin.planning_area)
SELECT p.*, 'point' AS geom_type
FROM public.raw_points_of_interest p, pa
WHERE ST_Intersects(p.geom, pa.geom)
UNION ALL
SELECT a.*, 'area'  AS geom_type
FROM public.raw_areas_of_interest a, pa
WHERE ST_Intersects(a.geom, pa.geom);

-- Helpful indexes for frequent filters and spatial joins
CREATE INDEX IF NOT EXISTS unified_mat_tags         ON staging.unified_features_mat (amenity, shop, leisure);
CREATE INDEX IF NOT EXISTS unified_mat_geom_gix     ON staging.unified_features_mat USING GIST (geom);
CREATE INDEX IF NOT EXISTS unified_mat_osm_geomtype ON staging.unified_features_mat (osm_id, geom_type);

-- =========================================================
-- 2) Config landing & load CSV
--    - Configuration-driven classification. The CSV defines classes and SQL snippets or files.
--    - Columns:
--        class_a: logical class name (unique key)
--        sql: either a WHERE clause, a path to a SQL file (/sql/class_a_defs/*.sql), or empty
--        polygon_policy: how to represent polygons (as area, all points, or points if small)
--        point_if_small_area: threshold for "point_if_small" policy (defaults to 1000 m2)
-- =========================================================
DROP TABLE IF EXISTS staging.class_a_config CASCADE;

CREATE TABLE staging.class_a_config (
  class_a      text PRIMARY KEY,
  sql          text,
  polygon_policy  text DEFAULT 'area',          -- 'area' | 'point_all' | 'point_if_small'
  point_if_small_area  double precision DEFAULT :DEFAULT_POINT_IF_SMALL_AREA    -- m2 threshold for point_if_small
);

\copy staging.class_a_config (class_a, sql,polygon_policy,point_if_small_area) FROM '/sql/class_a_config.csv' WITH (FORMAT csv, HEADER true, NULL '');

-- Trim whitespace for robust parsing
UPDATE staging.class_a_config
SET sql = btrim(sql);

-- Normalize OSM key syntax: change "key:subkey" to "key_subkey" to match column names
UPDATE staging.class_a_config
SET sql = replace(btrim(sql), ':', '_');

-- Validate allowed policy values early
ALTER TABLE staging.class_a_config
  ADD CONSTRAINT polygon_policy_chk
  CHECK (polygon_policy IS NULL OR polygon_policy IN ('area','point_all','point_if_small'));


-- load class b config - used to group class_a & b
DROP TABLE IF EXISTS staging.class_b_config CASCADE;

CREATE TABLE staging.class_b_config (
  active       boolean,
  class_a      text PRIMARY KEY,
  class_b      text 
);


\copy staging.class_b_config (active, class_a, class_b) FROM '/sql/class_b_config.csv' WITH (FORMAT csv, HEADER true, NULL '');

UPDATE staging.class_b_config
SET
  class_a = btrim(class_a),
  class_b = btrim(class_b);

DELETE FROM staging.class_b_config b
WHERE active IS NOT TRUE;  -- deletes FALSE and NULL


-- remove inactive class_a keys from class_a_config: ones that are not in class_b_config  
  
DELETE FROM staging.class_a_config a
WHERE NOT EXISTS (
  SELECT 1
  FROM staging.class_b_config b
  WHERE b.class_a = a.class_a
);


-- =========================================================
-- 3) Classification
--    - For each configured class:
--        * If sql starts with WHERE -> wrap a simple SELECT from unified_features_mat.
--        * If sql is a path -> read the file and expect a full SELECT/WITH returning (osm_id,name,geom).
--        * If sql is empty -> use default file /sql/class_a_defs/default/<class_a>.sql.
--    - Results are appended to staging.rule_hits (osm_id, class_a, name, geom).
-- =========================================================
DROP TABLE IF EXISTS staging.rule_hits CASCADE;
CREATE TABLE staging.rule_hits (
  osm_id   text,
  class_a  text,
  name     text,
  geom     geometry
);

DO $$
DECLARE
  rec RECORD;
  resolved_path text;
  snippet       text;
  first_kw      text;
  sql_text      text;
BEGIN
  FOR rec IN
    SELECT *
    FROM staging.class_a_config
    ORDER BY class_a
  LOOP
    sql_text := NULL;

    -- Case 1: sql begins with WHERE -> convert to SELECT against unified_features_mat
    IF lower(ltrim(rec.sql)) LIKE 'where%' THEN
        sql_text := format($fmt$
            INSERT INTO staging.rule_hits (osm_id, class_a, name, geom)
            SELECT u.osm_uid AS osm_id, %L AS class_a, u.name, u.geom
            FROM staging.unified_features_mat u
            %s;
        $fmt$, rec.class_a, rec.sql);  

    -- Case 2: sql is a path to a .sql file (full query)
    ELSIF rec.sql ~ '^/sql/class_a_defs/.*\.sql$' THEN
        resolved_path := rec.sql;

    -- Case 3: empty -> fall back to default path
    ELSIF rec.sql IS NULL THEN
        resolved_path := '/sql/class_a_defs/default/' || rec.class_a || '.sql';

    ELSE
        RAISE EXCEPTION 'Invalid sql value for class %: % (must start with WHERE, be /sql/class_a_defs/*.sql, or be empty)', rec.class_a, rec.sql;
    END IF;

    -- If we haven't built sql_text yet, we need to read and wrap the file contents
    IF sql_text IS NULL THEN
        snippet := pg_read_file(resolved_path);

        -- Detect leading keyword for safety (expect SELECT/WITH)
        first_kw := lower(regexp_replace(snippet, '^\s*', ''));
        first_kw := substring(first_kw from '^[a-z]+');

        IF first_kw IN ('select','with') THEN
          -- FULL-SQL mode: snippet must yield (osm_id, name, geom)
          -- We join back to unified_features_mat by osm_id and ST_Equals to guarantee geometry identity.
          sql_text := format($fmt$
              INSERT INTO staging.rule_hits (osm_id, class_a, name, geom)
              SELECT u.osm_uid AS osm_id, %L AS class_a, subq.name, subq.geom
              FROM (
                %s
              ) AS subq
              JOIN staging.unified_features_mat u ON u.osm_id = subq.osm_id AND ST_Equals(u.geom, subq.geom);
          $fmt$, rec.class_a, snippet);
        ELSE
          RAISE EXCEPTION 'Unrecognized snippet for class % at % (expected SELECT/WITH)', rec.class_a, resolved_path;
        END IF;
    END IF;
    
    -- Execute the prepared INSERT for this class
    EXECUTE sql_text;
  END LOOP;
END$$;

-- join class_b 

-- ALTER TABLE staging.rule_hits
--   ADD COLUMN IF NOT EXISTS class_b text;

-- UPDATE staging.rule_hits r
-- SET class_b = b.class_b
-- FROM staging.class_b_config b
-- WHERE b.class_a = r.class_a;

-- =========================================================
-- 3a) Debugging / Transparency: Multi-class features
--      - List OSM objects that matched more than one class_a.
--      - Useful for troubleshooting overlapping rules.
-- =========================================================
CREATE OR REPLACE VIEW staging.multiclass_features AS
SELECT
  r.osm_id,
  r.name,
  COUNT(DISTINCT r.class_a) AS n_classes,
  ARRAY_AGG(DISTINCT r.class_a ORDER BY r.class_a) AS classes,
  ST_Centroid(ST_Collect(r.geom)) AS geom
FROM staging.rule_hits r
GROUP BY r.osm_id, r.name
HAVING COUNT(DISTINCT r.class_a) > 1;

-- Indexes for faster downstream joins/filters
CREATE INDEX IF NOT EXISTS rule_hits_class    ON staging.rule_hits (class_a);
CREATE INDEX IF NOT EXISTS rule_hits_geom_gix ON staging.rule_hits USING GIST (geom);

-- =========================================================
-- 4) Geometry normalization per-class policy
--      - Apply polygon-to-point conversion policies as configured per class.
--      - Policies:
--          * area            : keep polygon geometries as-is
--          * point_all       : convert all polygons to points via PointOnSurface
--          * point_if_small  : convert polygons smaller than threshold to points
--      - Non-polygons are left unchanged for point_* policies.
-- =========================================================
DROP MATERIALIZED VIEW IF EXISTS staging.rule_hits_normalized CASCADE;
CREATE MATERIALIZED VIEW staging.rule_hits_normalized AS
SELECT
  r.osm_id,
  r.class_a,
  r.name,
  CASE
    WHEN COALESCE(c.polygon_policy, 'area') = 'area' THEN r.geom

    WHEN COALESCE(c.polygon_policy, 'area') = 'point_all'
         AND GeometryType(r.geom) IN ('POLYGON','MULTIPOLYGON')
      THEN ST_PointOnSurface(r.geom)
    WHEN COALESCE(c.polygon_policy, 'area') = 'point_all'
      THEN r.geom  -- don't touch non-polygons

    WHEN COALESCE(c.polygon_policy, 'area') = 'point_if_small'
         AND GeometryType(r.geom) IN ('POLYGON','MULTIPOLYGON')
         -- Use geography (after 4326 transform) for an area threshold in m2
         AND ST_Area(ST_Transform(r.geom, 4326)::geography) < COALESCE(c.point_if_small_area, 1000)

      THEN ST_PointOnSurface(r.geom)
    WHEN COALESCE(c.polygon_policy, 'area') = 'point_if_small'
      THEN r.geom  -- non-polygon or no threshold -> no change

    ELSE r.geom    -- unknown policy -> keep original
  END AS geom
FROM staging.rule_hits r
JOIN staging.class_a_config c USING (class_a);

-- =========================================================
-- 5) Fix invalid geometries
--    - Make polygonal geometries valid; keep other types unchanged.
--    - Drop empty geometries early to avoid issues later on.
-- =========================================================
DROP MATERIALIZED VIEW IF EXISTS staging.classified_valid CASCADE;
CREATE MATERIALIZED VIEW staging.classified_valid AS
SELECT
  ROW_NUMBER() OVER (
    ORDER BY osm_id, class_a, name
  )::bigint AS a_id,
  osm_id,
  class_a,
  name,
  CASE
    WHEN NOT ST_IsValid(geom) AND GeometryType(geom) IN ('POLYGON','MULTIPOLYGON')
      THEN ST_CollectionExtract(ST_MakeValid(geom), 3)  -- 3 = POLYGON/MULTIPOLYGON
    ELSE geom
  END AS geom
FROM staging.rule_hits_normalized
WHERE NOT ST_IsEmpty(geom);

CREATE INDEX IF NOT EXISTS classified_valid_class 
  ON staging.classified_valid (class_a);

CREATE INDEX IF NOT EXISTS classified_valid_geom_gix 
  ON staging.classified_valid USING GIST (geom);

-- =========================================================
-- 6) Drop points of same class that INTERSECT polygons of same class
--    - Keep all polygons.
--    - Keep only those points that do NOT intersect a polygon of the same class.
--    - Publish to model schema as two MVs and a convenience UNION ALL view.
-- =========================================================
CREATE SCHEMA IF NOT EXISTS model;  -- publishable/consumable outputs

DROP MATERIALIZED VIEW IF EXISTS model.amenities_polygons CASCADE;
CREATE MATERIALIZED VIEW model.amenities_polygons AS
SELECT *
FROM staging.classified_valid
WHERE GeometryType(geom) IN ('POLYGON','MULTIPOLYGON');

CREATE INDEX IF NOT EXISTS amenities_polygons_geom_gix
  ON model.amenities_polygons USING GIST (geom);
CREATE INDEX IF NOT EXISTS amenities_polygons_class
  ON model.amenities_polygons (class_a);
CREATE UNIQUE INDEX IF NOT EXISTS amenities_polygons_a_id
  ON model.amenities_polygons (a_id);

DROP MATERIALIZED VIEW IF EXISTS model.amenities_points CASCADE;
CREATE MATERIALIZED VIEW model.amenities_points AS
WITH pts AS (
  SELECT *
  FROM staging.classified_valid
  WHERE GeometryType(geom) IN ('POINT','MULTIPOINT')
),
filtered AS (
  SELECT p.*
  FROM pts p
  WHERE NOT EXISTS (
    SELECT 1
    FROM model.amenities_polygons a
    WHERE a.class_a = p.class_a
      AND ST_Intersects(a.geom, p.geom)
  )
)
SELECT * FROM filtered;

CREATE INDEX IF NOT EXISTS amenities_points_geom_gix
  ON model.amenities_points USING GIST (geom);
CREATE INDEX IF NOT EXISTS amenities_points_class
  ON model.amenities_points (class_a);
CREATE UNIQUE INDEX IF NOT EXISTS amenities_points_a_id
  ON model.amenities_points (a_id);

-- Convenience read endpoint (non-materialized): combine points + polygons
CREATE OR REPLACE VIEW model.amenities AS
SELECT * FROM model.amenities_points
UNION ALL
SELECT * FROM model.amenities_polygons;



-- =========================================================
-- 7) Roads & Entrances + parent tags from unified_features_mat
--    - Roads: select OSM line features with a non-empty 'highway' tag.
--    - Entrances:
--        * For polygon amenities: intersect polygon boundary with roads to find probable entrances.
--            - Point intersections -> entrance points
--            - Linear overlaps -> take the midpoint as an entrance point
--        * For polygons with NO boundary-road intersection -> use centroid as a fallback entrance
--        * For point amenities -> treat the point itself as an entrance
--      Each entrance receives a deterministic entrance_id based on (parent, road, coordinates).
-- =========================================================

-- Roads (only features having highway)
DROP MATERIALIZED VIEW IF EXISTS model.roads CASCADE;
CREATE MATERIALIZED VIEW model.roads AS
SELECT
  osm_id AS road_osm_id,
  highway,
  name,
  geom
FROM public.raw_lines_roads
WHERE highway IS NOT NULL AND highway <> '';

CREATE INDEX IF NOT EXISTS roads_geom_gix ON model.roads USING GIST (geom);
CREATE INDEX IF NOT EXISTS roads_highway  ON model.roads (highway);

-- Entrances with parent tags (parent tags join is prepared but commented out below)
DROP MATERIALIZED VIEW IF EXISTS model.entrances CASCADE;
CREATE MATERIALIZED VIEW model.entrances AS
WITH polys AS (
  -- Only polygon amenities are candidates for boundary-road intersections
  SELECT
    p.a_id AS parent_a_id,
    p.osm_id AS parent_osm_id,
    p.class_a,
    p.name,
    p.geom
  FROM model.amenities_polygons p
),
ix AS (
  -- Intersections of polygon boundary with roads (may produce points and/or lines)
  SELECT
    p.parent_a_id,
    p.parent_osm_id,
    p.class_a,
    p.name,
    r.road_osm_id,
    r.highway,  -- road highway
    ST_Intersection(ST_Boundary(p.geom), r.geom) AS gi
  FROM polys p
  JOIN model.roads r
    ON ST_Intersects(r.geom, ST_Boundary(p.geom))
),
ix_pts AS (
  -- Extract discrete entrance points from point intersections
  SELECT
    parent_a_id,
    parent_osm_id,
    class_a,
    name,
    road_osm_id,
    highway,
    (dp).geom::geometry(Point, :TARGET_SRID) AS geom,
    'road_intersection'::text        AS source
  FROM ix
  CROSS JOIN LATERAL ST_Dump(ST_CollectionExtract(gi, 1)) AS dp
),
ix_line_mid AS (
  -- For linear overlaps (shared edges), take the midpoint as a plausible entrance location
  SELECT
    parent_a_id,
    parent_osm_id,
    class_a,
    name,
    road_osm_id,
    highway,
    ST_LineInterpolatePoint((dl).geom, 0.5)::geometry(Point, :TARGET_SRID) AS geom,
    'road_intersection_line'::text                                  AS source
  FROM ix
  CROSS JOIN LATERAL ST_Dump(ST_CollectionExtract(gi, 2)) AS dl
),
hits AS (
  SELECT * FROM ix_pts
  UNION ALL
  SELECT * FROM ix_line_mid
),
nohit_polys AS (
  -- Polygons with no boundary-road intersection -> fallback entrance at centroid
  SELECT
    p.a_id AS parent_a_id,
    p.osm_id AS parent_osm_id,
    p.class_a,
    p.name,
    NULL::bigint                              AS road_osm_id,
    NULL::text                                AS highway,
    ST_Centroid(p.geom)::geometry(Point,:TARGET_SRID) AS geom,
    'centroid'::text                          AS source
  FROM model.amenities_polygons p
  WHERE NOT EXISTS (
    SELECT 1
    FROM model.roads r
    WHERE ST_Intersects(r.geom, ST_Boundary(p.geom))
  )
),
all_points AS (
  -- Include all point amenities as entrances (they already are points)
  SELECT
    a_id AS parent_a_id,
    osm_id       AS parent_osm_id,
    class_a,
    name,
    NULL::bigint AS road_osm_id,
    NULL::text   AS highway,
    geom::geometry(Point, :TARGET_SRID) AS geom,
    'point'::text               AS source
  FROM model.amenities_points
),
u AS (
  -- Union of all entrance candidates
  SELECT * FROM hits
  UNION ALL
  SELECT * FROM nohit_polys
  UNION ALL
  SELECT * FROM all_points
)

SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      u.parent_a_id,
      u.parent_osm_id,
      u.road_osm_id,
      u.class_a
  )::bigint AS entrance_id,                 
  u.parent_a_id,
  u.parent_osm_id,
  u.class_a,
  u.geom,
  u.source,
  staging.h3_from_geom(ST_Transform(u.geom, 4326), 10) AS h3_cell

  -- Optional: bring parent feature tags (commented; uncomment when needed)
  -- uf.amenity               AS parent_amenity,
  -- uf.shop                  AS parent_shop,
  -- uf.leisure               AS parent_leisure,
  -- uf.office                AS parent_office,
  -- uf.tourism               AS parent_tourism,
  -- uf.healthcare            AS parent_healthcare,
  -- uf.place                 AS parent_place,
  -- uf.natural               AS parent_natural,
  -- uf.landuse               AS parent_landuse,
  -- uf.boundary              AS parent_boundary,
  -- uf.post_office           AS parent_post_office,
  -- uf.building              AS parent_building,
  -- uf.man_made              AS parent_man_made,
  -- uf.sport                 AS parent_sport,
  -- uf.railway               AS parent_railway,
  -- uf.public_transport      AS parent_public_transport,
  -- uf.highway               AS parent_highway,
  -- uf.bus                   AS parent_bus,
  -- uf.tram                  AS parent_tram,
  -- uf.train                 AS parent_train,
  -- uf.origin                AS parent_origin,
  -- uf.isced_level           AS parent_isced_level,
  -- uf.healthcare_speciality AS parent_healthcare_speciality
FROM u;
-- LEFT JOIN staging.unified_features_mat uf
--   ON uf.osm_id = u.parent_osm_id;

-- Indexes for common access patterns
CREATE INDEX IF NOT EXISTS entrances_geom_gix    ON model.entrances USING GIST (geom);
CREATE INDEX IF NOT EXISTS entrances_parent_idx  ON model.entrances (parent_a_id);
CREATE INDEX IF NOT EXISTS entrances_class_idx   ON model.entrances (class_a);
CREATE INDEX IF NOT EXISTS entrances_id_idx      ON model.entrances (entrance_id);
CREATE INDEX IF NOT EXISTS entrances_h3_idx      ON model.entrances (h3_cell);
