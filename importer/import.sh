#!/bin/sh
# ---------------------------------------------------------
#
# Purpose
#   Download a PBF extract and import it into Postgres using osm2pgsql (flex output) with given style.lua.
#   Provide minimal runtime context (source URL, target SRID, DB connection params).
#
# Requirements
#   curl, osm2pgsql, a reachable PostgreSQL instance with proper credentials.
#   style.lua available at /importer/style.lua (mounted in the container/host as needed).
#
# Environment variables (must be set by the caller)
#   PBF_URL      : HTTP(S) URL of the .pbf file to import (e.g., Geofabrik extract).
#   PGDATABASE   : Target database name (e.g., osm)
#   PGUSER       : DB username
#   PGHOST       : DB host (e.g., localhost or container name)
#   PGPORT       : DB port (e.g., 5432)
#   TARGET_SRID  : Informational only here; should match the SRID used in style.lua (default 3857 there).
#   (optional) PGPASSWORD or .pgpass for non-interactive auth
#

set -e  # exit immediately on error

# --- Print context info ---
echo "Starting OSM import"
echo "Downloading PBF from: $PBF_URL"
echo "Target SRID: $TARGET_SRID"
echo "Database: $PGDATABASE@$PGHOST:$PGPORT (user: $PGUSER)"

# Location for the temporary PBF
PBF_FILE="/data/osm-import.osm.pbf"

# --- Download step ---
if [ -f "$PBF_FILE" ]; then
  echo "PBF already exists at $PBF_FILE; skipping download."
else
  echo "Downloading PBF from: $PBF_URL"
  # -f fails on HTTP errors; -L follows redirects; -o writes to the file
  curl -fL "$PBF_URL" -o "$PBF_FILE"
fi

osm2pgsql \
  --create \
  --input-reader=pbf \
  --database "$PGDATABASE" \
  -U "$PGUSER" \
  --host "$PGHOST" \
  --port "$PGPORT" \
  --style=/importer/style.lua \
  --output=flex \
  --number-processes "${OSM2PGSQL_JOBS:-$(nproc)}" \
  "$PBF_FILE"

echo "Import Complete!"