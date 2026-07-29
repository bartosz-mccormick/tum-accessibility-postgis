docker compose exec -T db sh -lc '
  set -e
 
  if [ -f /data/pop_raster.tif ]; then
    echo "Raster already exists at /data/pop_raster.tif; skipping download."
  else
    echo "Downloading raster from: $POP_URL"
    curl -fL "$POP_URL" -o /data/pop_raster.tif
  fi

  echo "Dropping existing table public.raw_pop_raster (if any)"
  PGPASSWORD="$POSTGRES_PASSWORD" \
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "DROP TABLE IF EXISTS public.raw_pop_raster CASCADE;"

  echo "Importing raster (tiled 256x256) into public.raw_pop_raster"
  raster2pgsql -I -M -t 256x256 /data/pop_raster.tif public.raw_pop_raster \
  | PGPASSWORD="$POSTGRES_PASSWORD" \
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"

  echo "Processing Raster"
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -p "${PGPORT:-5432}" \
  -v ON_ERROR_STOP=1 -f /sql/pop_grid_from_raster.sql
'
