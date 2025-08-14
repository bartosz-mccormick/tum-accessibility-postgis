#!/bin/sh
set -e

# Print some context info
echo "Starting OSM import"
echo "Downloading PBF from: $PBF_URL"
echo "Target SRID: $TARGET_SRID"

PBF_FILE="/tmp/osm-import.pbf"

echo "Downloading PBF..."
curl -L "$PBF_URL" -o "$PBF_FILE"

# Run osm2pgsql with flex style
osm2pgsql \
  --create \
  --input-reader=pbf \
  --database "$PGDATABASE" \
  -U "$PGUSER" \
  --host "$PGHOST" \
  --port "$PGPORT" \
  --style=/importer/style.lua \
  --output=flex \
  "$PBF_FILE"

echo "Import Complete!"
