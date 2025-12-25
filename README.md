# MeshMap.net

MeshMap shows Meshtastic nodes in near real time by consuming MQTT traffic, enriching it, and serving the data over HTTP for a static web client.

## What it does
- Listens to Meshtastic MQTT topics, decrypts packets with the default PSK, and builds an in-memory node database (`meshobserv`).
- Exposes `nodes.json` over HTTP from `meshobserv`; the frontend consumes this to render the map.
- Generates coverage heatmap points from `nodes.json` using elevation APIs and serves them via `/coverage_points.geojson` (`coverage` service).
- Serves the website via Caddy and proxies `/nodes.json` and `/coverage_points.geojson` to the backend services.
- Optional Mosquitto broker is included in docker-compose; data is persisted to a volume and port 1883 is published.

## Services (docker-compose)
- `meshobserv`: Go MQTT consumer; in-memory nodes; HTTP on :80 (`/nodes.json`, `/healthz`). Env:
  - `MQTT_BROKER` (default `tcp://mqtt.meshtastic.org:1883`)
  - `MQTT_USERNAME` (default `meshdev`)
  - `MQTT_PASSWORD` (default `large4cats`)
- `coverage`: Go service; fetches nodes from `NODES_URL`, generates GeoJSON in memory, serves `/coverage_points.geojson` and `/healthz` on :80. Env:
  - `NODES_URL` (default `http://meshobserv/nodes.json`)
  - `COVERAGE_HTTP_ADDR` (default `:80`)
  - `CACHE_DB_PATH` (default `/cache/elevation_cache.duckdb`, backed by `coverage_cache` volume)
- `web`: Caddy serves static `website/` and proxies API endpoints. Mounts `configs/Caddyfile`.
- `mosquitto`: Optional broker (`eclipse-mosquitto:2`) with persistence (`mosquitto_data` volume), autosave every 30s, port `1883:1883`.

Named volumes:
- `coverage_cache`: persistent DuckDB cache for elevation data.
- `mosquitto_data`: persistent Mosquitto data.

## Running locally
1. Build & start: `docker compose up --build`
2. Open the site: `http://localhost/`
3. Data endpoints (proxied by Caddy):
   - `http://localhost/nodes.json`
   - `http://localhost/coverage_points.geojson`

## Configuration notes
- `meshobserv` operates fully in memory; no node file is written.
- Coverage uses external elevation APIs (Open-Elevation/OpenTopodata) with simple rate limiting and cached results in DuckDB.
- Default Meshtastic credentials/keying follow upstream defaults; adjust `MQTT_*` env vars as needed.

## Contributing / Fork notes
This fork uses module path `github.com/D4rk4/meshmap.net`. Go builds and `docker compose` are configured for that path. Pull requests and issues are welcome. Review LICENSE before redistributing. Live site: https://mthub.monteops.com. Original project by Brian Shea (brianshea2): https://github.com/brianshea2/meshmap.net
