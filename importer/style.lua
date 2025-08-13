-- LIST A (primary classification keys)
local KEYS_CLASS_MAIN = {
    'shop','amenity','office','tourism','healthcare','place','natural','landuse',
    'leisure','boundary','post_office','building','man_made','sport',
    'railway','public_transport','highway'
}

-- LIST B (refinement keys) – not used to decide, but stored in tags
local KEYS_CLASS_REFINE = {
    'bus','tram','train','origin','isced:level','healthcare:speciality'
}

-- Turn LIST A into a fast lookup table
local KEEP_KEYS = {}
for _, k in ipairs(KEYS_CLASS_MAIN) do
    KEEP_KEYS[k] = true
end

-- Return true if any LIST A key exists and has a value
-- can reduce size of import further by using: make_check_values_func
local function keep_feature(tags)
    for k, _ in pairs(KEEP_KEYS) do
        if tags[k] and tags[k] ~= '' then
            return true
        end
    end
    return false
end




  -- Set this to the projection you want to use
local srid = 3857

local tables = {}


local function sanitize_key(key)
    return string.gsub(key, ":", "_")
end

local function unsanitize_key(key)
    return string.gsub(key, "_", ":")
end

local columns_points = {}

-- Add all classification + refinement keys as text columns
for _, k in ipairs(KEYS_CLASS_MAIN) do
    table.insert(columns_points, { column = sanitize_key(k), type = 'text' })
end
for _, k in ipairs(KEYS_CLASS_REFINE) do
    table.insert(columns_points, { column = sanitize_key(k), type = 'text' })
end

-- Add geometry and optional ID columns
table.insert(columns_points, { column = 'geom', type = 'point', projection = srid, not_null = true })
table.insert(columns_points, { column = 'osm_id', type = 'int8' })
table.insert(columns_points, { column = 'name', type = 'text' })


-- For areas: copy and swap geometry type
local columns_poly = {}
for _, col in ipairs(columns_points) do
    if col.column == 'geom' then
        table.insert(columns_poly, { column = 'geom', type = 'polygon', projection = srid, not_null = true })
    else
        table.insert(columns_poly, col)
    end
end



tables.raw_points_of_interest = osm2pgsql.define_node_table('raw_points_of_interest', columns_points)
tables.raw_areas_of_interest = osm2pgsql.define_area_table('raw_areas_of_interest', columns_poly)



-- Helper function that looks at the tags and decides if this is possibly
-- an area.
local function has_area_tags(tags)
    if tags.area == 'yes' then
        return true
    end
    if tags.area == 'no' then
        return false
    end

    return tags.aeroway
        or tags.amenity
        or tags.building
        or tags.harbour
        or tags.historic
        or tags.landuse
        or tags.leisure
        or tags.man_made
        or tags.military
        or tags.natural
        or tags.office
        or tags.place
        or tags.power
        or tags.public_transport
        or tags.shop
        or tags.sport
        or tags.tourism
        or tags.water
        or tags.waterway
        or tags.wetland
        or tags['abandoned:aeroway']
        or tags['abandoned:amenity']
        or tags['abandoned:building']
        or tags['abandoned:landuse']
        or tags['abandoned:power']
        or tags['area:highway']
        or tags['building:part']
end




local column_names={}
local n=0

for k,v in pairs(columns_points) do
  n=n+1
  if v ~= "geom" then
    column_names[n]=k
  end
end


local function extract_tag_fields(tags)
    local row = {}
    for _, k in ipairs(KEYS_CLASS_MAIN) do
        row[unsanitize_key(k)] = tags[k]
    end
    return row
end


function osm2pgsql.process_node(object)
    if not keep_feature(object.tags) then return end

    local row = extract_tag_fields(object.tags)
    row.geom = object:as_point()

    tables.raw_points_of_interest:insert(row)
end

function osm2pgsql.process_way(object)
    if not keep_feature(object.tags) then return end

    if object.is_closed and has_area_tags(object.tags) then

        local row = extract_tag_fields(object.tags)
        row.geom = object:as_polygon()

        tables.raw_areas_of_interest:insert(row)
    else
        -- skip open ways (lines) for this style
        return
    end
end


function osm2pgsql.process_relation(object)
    if not keep_feature(object.tags) then return end

    local relation_type = object:grab_tag('type')
    if relation_type == 'multipolygon' and has_area_tags(object.tags) then

        -- From the relation we get multipolygons...
        local mp = object:as_multipolygon()
        -- ...and split them into polygons which we insert into the table
        for geom in mp:geometries() do
            local row = extract_tag_fields(object.tags) -- could likely be more efficient if moved outside of the loop
            row.geom = geom
            tables.raw_areas_of_interest:insert(row)
        end
    else
        return
    end
end