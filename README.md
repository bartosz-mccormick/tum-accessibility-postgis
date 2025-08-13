# tum-accessibility-postgis
Quickly create a PostGIS database ready for accessibility analyses. Import OSM data via `osm2pgsql` , then clean, de-duplicate, and classify into analysis-ready categories.

## Quick start

1. Create a `.env` file by making a copying `.env.example`, adjust the variables as desired
2. Create / start the database: `docker compose up -d db`
    - Note: if you'd like to delete your database: `docker volume rm tum-accessibility-postgis_pgdata`
3. Download `.pbf`file and import it into the database using `osm2pgsql`: `docker compose --profile import up`



