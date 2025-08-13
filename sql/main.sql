
-- =========================================================
-- 0) Extensions & Schemas
-- =========================================================
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS staging;

-- =========================================================
-- 1) Unified features as MATERIALIZED VIEW (fast & indexable)
-- =========================================================
DROP MATERIALIZED VIEW IF EXISTS staging.unified_features_mat CASCADE;
CREATE MATERIALIZED VIEW staging.unified_features_mat AS
SELECT
  *,
  'point' AS geom_type
FROM public.raw_points_of_interest
UNION ALL
SELECT
  *,
  'area' AS geom_type
FROM public.raw_areas_of_interest;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS unified_mat_tags ON staging.unified_features_mat (amenity, shop, leisure);
CREATE INDEX IF NOT EXISTS unified_mat_geom_gix ON staging.unified_features_mat USING GIST (geom);
CREATE INDEX IF NOT EXISTS unified_mat_osm_geomtype ON staging.unified_features_mat (osm_id, geom_type);

-- =========================================================
-- 2) Config landing & load CSV
--    (/sql/class_a_config.csv with columns: class_a,source)
-- =========================================================
DROP TABLE IF EXISTS staging.class_a_config;

CREATE TABLE staging.class_a_config (
  class_a text PRIMARY KEY,
  sql  text
);

\copy staging.class_a_config (class_a, sql) FROM '/sql/class_a_config.csv' WITH (FORMAT csv, HEADER true, NULL '');

-- trim leading and trailing whitespace
UPDATE staging.class_a_config
SET sql = btrim(sql);

-- =========================================================
-- 3) Classification
--    - Read each snippet file for a class
--    - Support WHERE-only snippets (wrapped) and FULL-SQL (SELECT/WITH)
--    - Write to staging.rule_hits (osm_id, class_a, priority, geom)
-- =========================================================
DROP TABLE IF EXISTS staging.rule_hits;
CREATE TABLE staging.rule_hits (
  osm_id   bigint,
  class_a  text,
  name text,
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
    IF lower(ltrim(rec.sql)) LIKE 'where%' THEN
        -- handle WHERE case here
        sql_text := format($fmt$
            INSERT INTO staging.rule_hits (osm_id, class_a, name, geom)
            SELECT osm_id, %L AS class_a, name, geom
            FROM staging.unified_features_mat
            %s;
        $fmt$, rec.class_a, rec.sql);  
    ELSIF rec.sql ~ '^/sql/class_a_defs/.*\.sql$' THEN
        resolved_path := rec.sql;
    ELSIF rec.sql IS NULL THEN
        -- Default to /sql/class_a_defs/default/<class_a>.sql if sql is empty
        resolved_path := '/sql/class_a_defs/default/' || rec.class_a || '.sql';
    ELSE
        RAISE EXCEPTION 'Invalid sql value for class %: % (must start with WHERE, be /sql/class_a_defs/*.sql, or be empty)', rec.class_a, rec.sql;
    END IF;
    -- handle full SELECT/WITH/etc. case here
    IF sql_text IS NULL THEN
        -- NOTE: pg_read_file requires superuser (true for default POSTGRES_USER)
        snippet := pg_read_file(resolved_path);

        -- Detect if snippet starts with SELECT/WITH
        first_kw := lower(regexp_replace(snippet, '^\s*', ''));
        first_kw := substring(first_kw from '^[a-z]+');

        IF first_kw IN ('select','with') THEN
        -- FULL-SQL mode: snippet must yield (osm_id,name, geom)
            sql_text := format($fmt$
                INSERT INTO staging.rule_hits (osm_id, class_a, name, geom)
                SELECT osm_id, %L AS class_a,name, geom
                FROM (
                %s
                ) AS subq;
            $fmt$, rec.class_a, snippet);
        ELSE
            RAISE EXCEPTION 'Unrecognized snippet for class % at % (expected SELECT/WITH)', rec.class_a, resolved_path;
        END IF;
    END IF;
    
    EXECUTE sql_text;
  END LOOP;
END$$;

CREATE INDEX IF NOT EXISTS rule_hits_class ON staging.rule_hits (class_a);
CREATE INDEX IF NOT EXISTS rule_hits_geom_gix ON staging.rule_hits USING GIST (geom);

-- -- =========================================================
-- -- 4) Favor polygons over points for the SAME osm_id
-- --    - Keep ALL polygons (including multiple parts from multipolygons)
-- --    - Drop ONLY the point if a polygon with the same osm_id exists
-- -- =========================================================
-- DROP MATERIALIZED VIEW IF EXISTS staging.classified_pref_area CASCADE;
-- CREATE MATERIALIZED VIEW staging.classified_pref_area AS
-- WITH areas AS (
--   SELECT DISTINCT osm_id
--   FROM staging.rule_hits
--   WHERE GeometryType(geom) IN ('POLYGON','MULTIPOLYGON')
-- ),
-- kept_points AS (
--   SELECT r.*
--   FROM staging.rule_hits r
--   WHERE GeometryType(r.geom) = 'POINT'
--     AND NOT EXISTS (SELECT 1 FROM areas a WHERE a.osm_id = r.osm_id)
-- ),
-- kept_areas AS (
--   SELECT r.*
--   FROM staging.rule_hits r
--   WHERE GeometryType(r.geom) IN ('POLYGON','MULTIPOLYGON')
-- )
-- SELECT * FROM kept_areas
-- UNION ALL
-- SELECT * FROM kept_points;

-- CREATE INDEX IF NOT EXISTS classified_pref_area_class ON staging.classified_pref_area (class_a);
-- CREATE INDEX IF NOT EXISTS classified_pref_area_geom_gix ON staging.classified_pref_area USING GIST (geom);

-- -- =========================================================
-- -- 5) Fix invalid geometries
-- -- =========================================================
-- DROP MATERIALIZED VIEW IF EXISTS staging.classified_valid CASCADE;
-- CREATE MATERIALIZED VIEW staging.classified_valid AS
-- SELECT
--   osm_id, class_a, priority,
--   CASE
--     WHEN NOT ST_IsValid(geom) AND GeometryType(geom) LIKE 'POLYGON%' THEN ST_MakeValid(geom)
--     WHEN NOT ST_IsValid(geom) THEN ST_Buffer(geom, 0)   -- fallback for non-polys
--     ELSE geom
--   END AS geom
-- FROM staging.classified_pref_area;

-- CREATE INDEX IF NOT EXISTS classified_valid_class ON staging.classified_valid (class_a);
-- CREATE INDEX IF NOT EXISTS classified_valid_geom_gix ON staging.classified_valid USING GIST (geom);

-- -- =========================================================
-- -- 6) Drop points of same class that INTERSECT polygons of same class
-- --    - Keep all polygons
-- --    - Keep only those points that do NOT intersect a polygon of the same class
-- -- =========================================================
-- DROP MATERIALIZED VIEW IF EXISTS model.amenities_polygons CASCADE;
-- CREATE MATERIALIZED VIEW model.amenities_polygons AS
-- SELECT * FROM staging.classified_valid
-- WHERE GeometryType(geom) IN ('POLYGON','MULTIPOLYGON');

-- CREATE INDEX IF NOT EXISTS amenities_polygons_geom_gix ON model.amenities_polygons USING GIST (geom);
-- CREATE INDEX IF NOT EXISTS amenities_polygons_class ON model.amenities_polygons (class_a);

-- DROP MATERIALIZED VIEW IF NOT EXISTS model.amenities_points CASCADE;
-- CREATE MATERIALIZED VIEW model.amenities_points AS
-- WITH pts AS (
--   SELECT * FROM staging.classified_valid
--   WHERE GeometryType(geom) = 'POINT'
-- ),
-- filtered AS (
--   SELECT p.*
--   FROM pts p
--   WHERE NOT EXISTS (
--     SELECT 1
--     FROM model.amenities_polygons a
--     WHERE a.class_a = p.class_a
--       AND ST_Intersects(a.geom, p.geom)
--   )
-- )
-- SELECT * FROM filtered;

-- CREATE INDEX IF NOT EXISTS amenities_points_geom_gix ON model.amenities_points USING GIST (geom);
-- CREATE INDEX IF NOT EXISTS amenities_points_class ON model.amenities_points (class_a);

-- -- (Optional convenience view combining both, if you want a single read endpoint)
-- CREATE OR REPLACE VIEW model.amenities AS
-- SELECT class_a, geom FROM model.amenities_points
-- UNION ALL
-- SELECT class_a, geom FROM model.amenities_polygons;

-- -- Done
