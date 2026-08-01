# GeoSyncBackend

GeoSyncBackend is a Vapor 4 service for receiving device locations, relaying them to live dashboard clients, retaining tracking sessions in SQLite, and serving an offline vector map from an MBTiles archive.

It offers two ways to publish location:

- **REST** for simple HTTP clients and background jobs.
- **WebSocket** for low-latency device-to-admin updates and subscription counts.

The service stores clients, tracking sessions, and selected breadcrumb points in `db.sqlite`. Live connection state and each client's latest position are held in memory, so they are reset if the process restarts.

## Contents

- [Requirements](#requirements)
- [Run locally](#run-locally)
- [Configuration and data files](#configuration-and-data-files)
- [HTTP API](#http-api)
- [WebSocket API](#websocket-api)
- [Tracking and persistence](#tracking-and-persistence)
- [Docker](#docker)
- [Security and deployment](#security-and-deployment)

## Requirements

- Swift 6.3 or later (macOS 13+ for the package platform)
- An MBTiles vector-tile database named `osm-2020-02-10-v3.11_iran_tehran.mbtiles` in the working directory when map endpoints are needed
- Optional for map labels and icons: generated font glyphs and sprite files mounted at the paths described in [Map assets](#map-assets)
- Docker and Docker Compose, if running in a container

## Run locally

From the repository root:

```bash
swift build
swift run GeoSyncBackend serve --hostname 127.0.0.1 --port 8080
```

The server automatically creates and migrates `db.sqlite` in its current working directory on startup. Check that it is running:

```bash
curl http://127.0.0.1:8080/health
```

Expected response:

```json
{"status":"ok"}
```

Run the test suite with:

```bash
swift test
```

## Configuration and data files

There are no application-specific environment variables. Vapor's standard environment and logging options still apply; Docker Compose sets `LOG_LEVEL` to `debug` unless overridden.

| Item | Location | Purpose |
| --- | --- | --- |
| Application database | `db.sqlite` | Persistent clients, tracking sessions, and saved location points. Created automatically. |
| Map tiles | `osm-2020-02-10-v3.11_iran_tehran.mbtiles` | Read-only MBTiles SQLite database queried by the map routes. |
| Glyphs | `/fonts/_output/<fontstack>/<range>` | PBF font glyphs used by the generated MapLibre style. |
| Sprites | `/sprites/sprite.json`, `.png`, `@2x.json`, `@2x.png` | Map icon sprite metadata and images. |

### Map assets

The MBTiles archive must be available under its exact file name in the process working directory. The server logs whether it finds the file at startup. Map tile and metadata requests will fail if it is absent or does not contain the expected `tiles` and `metadata` tables.

The included map style refers to root-level `/fonts/...` and `/sprites/...` URLs. Tile rendering still works without those optional files, but text labels and POI icons will receive `404 Not Found` responses.

## HTTP API

Base URL examples below use `http://127.0.0.1:8080`. Send JSON bodies with `Content-Type: application/json`.

### `GET /`

Returns a plain-text service message.

```text
GeoSync live-location relay is running. Connect with WebSocket at /v1/live.
```

### `GET /health`

Health check.

**Response — `200 OK`**

```json
{"status":"ok"}
```

### `POST /v1/location/:clientId`

Publishes the current location for `:clientId`, updates subscribed WebSocket admins, and starts a tracking session automatically if the client does not already have one. REST-created sessions use the session tag `REST`.

**Request body**

```json
{
  "latitude": 35.6892,
  "longitude": 51.3890,
  "timestamp": "2026-08-01T12:00:00Z"
}
```

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `latitude` | number | Yes | Latitude supplied by the device. |
| `longitude` | number | Yes | Longitude supplied by the device. |
| `timestamp` | string | No | Device time; use ISO-8601 UTC, for example `2026-08-01T12:00:00Z`. |

```bash
curl -X POST http://127.0.0.1:8080/v1/location/device-123 \
  -H 'Content-Type: application/json' \
  -d '{"latitude":35.6892,"longitude":51.3890,"timestamp":"2026-08-01T12:00:00Z"}'
```

**Response — `200 OK`**

```text
ok
```

The REST heartbeat expires after 60 seconds without another location update. A background check runs every 30 seconds; on expiry, the client is marked offline, subscribed admins are notified, and its active session is closed. Call the stop endpoint when a client ends normally instead of waiting for timeout.

> REST coordinates are decoded as numbers but are not currently range-validated by this endpoint. Callers should send latitude in `[-90, 90]` and longitude in `[-180, 180]`.

### `POST /v1/location/:clientId/stop`

Explicitly ends a REST client's active tracking session, marks its cached location offline, and sends that offline update to subscribed WebSocket admins. It is safe to call when no session is active.

```bash
curl -X POST http://127.0.0.1:8080/v1/location/device-123/stop
```

**Response — `200 OK`**

```text
ok
```

### `GET /v1/history`

Returns the 10 most recent tracking sessions across all clients, newest first. Each session includes its saved location points.

**Response — `200 OK`**

```json
[
  {
    "id": "A1B2C3D4-E5F6-4A5B-9C8D-0123456789AB",
    "sessionTag": "REST",
    "totalDistanceKm": 1.42,
    "startTime": "2026-08-01T10:00:00Z",
    "endTime": "2026-08-01T10:12:00Z",
    "points": [
      {
        "id": "B1B2C3D4-E5F6-4A5B-9C8D-0123456789AB",
        "latitude": 35.6892,
        "longitude": 51.3890,
        "timestamp": "2026-08-01T10:00:00Z",
        "receivedAt": "2026-08-01T10:00:01Z"
      }
    ]
  }
]
```

### `GET /v1/history/:clientId`

Returns up to 20 tracking sessions for the given client, newest first. Each session includes its saved location points.

```bash
curl http://127.0.0.1:8080/v1/history/device-123
```

**Response — `200 OK`**: the same session array shape as `GET /v1/history`. An unknown client returns an empty array.

### `GET /v1/map/tiles/:z/:x/:y`

Returns a vector tile from the MBTiles file. Path parameters use normal XYZ/Slippy Map coordinates; the server converts `y` to MBTiles' TMS row internally.

```text
GET /v1/map/tiles/12/2632/1568
```

**Response**

- `200 OK`: gzipped Mapbox Vector Tile data, with `Content-Type: application/x-protobuf` and `Content-Encoding: gzip`
- `400 Bad Request`: `z`, `x`, or `y` is not an integer
- `404 Not Found`: tile is not present

### `GET /v1/map/style.json`

Returns a Mapbox Style Specification v8 JSON document for the internal vector tiles. Its URLs are generated from the request `Host` header and use `X-Forwarded-Proto` when present, which allows it to work behind an HTTPS reverse proxy.

Point MapLibre or Mapbox-compatible clients at:

```text
http://127.0.0.1:8080/v1/map/style.json
```

The style provides background, water, roads, buildings, place labels, and POI icon layers. It declares a source zoom range of 0–14.

### `GET /v1/map/metadata`

Returns all key-value records from the MBTiles `metadata` table.

**Response — `200 OK`**

```json
{
  "name": "Tehran",
  "format": "pbf"
}
```

### Map font and sprite endpoints

These routes support the URLs embedded in the style document:

| Endpoint | Success type | Notes |
| --- | --- | --- |
| `GET /fonts/:fontstack/:range` | `application/x-protobuf` | Streams `/fonts/_output/:fontstack/:range`; usually `:range` is a `.pbf` range such as `0-255.pbf`. |
| `GET /sprites/sprite.json` | `application/json` | Streams sprite metadata. |
| `GET /sprites/sprite.png` | `image/png` | Streams 1× sprite image. |
| `GET /sprites/sprite@2x.json` | `application/json` | Streams high-DPI sprite metadata. |
| `GET /sprites/sprite@2x.png` | `image/png` | Streams high-DPI sprite image. |

Every font and sprite route returns `404 Not Found` if its mapped file does not exist.

## WebSocket API

Connect to:

```text
ws://127.0.0.1:8080/v1/live
```

Use `wss://` when the service is exposed over TLS. Each frame is a JSON text message. On a new connection, the server immediately sends:

```json
{
  "type": "connected",
  "message": "Register as client or admin before sending other messages."
}
```

A socket can be registered as either one client or one admin, not both. The server does not validate a client ID's format; choose a stable unique string, such as a UUID generated once by the device.

### Client messages

#### `client.register`

Registers this socket as a location-producing client and automatically starts a new tracking session. If that client already has an active session, it is stopped first. `sessionTag` is stored with the new session when provided.

```json
{
  "type": "client.register",
  "clientId": "device-123",
  "sessionTag": "delivery-2026-08-01"
}
```

**Server messages**

```json
{"type":"client.registered","clientId":"device-123"}
```

The client also receives a `client.subscribers` message (shown below). If it reconnects while a cached location exists, subscribed admins also receive a live, online location update.

#### `client.location`

Publishes a location for the client registered on this socket. The `clientId` in the message must exactly match the socket's registration. WebSocket coordinate values are validated.

```json
{
  "type": "client.location",
  "clientId": "device-123",
  "latitude": 35.6892,
  "longitude": 51.3890,
  "timestamp": "2026-08-01T12:00:00Z"
}
```

| Field | Rule |
| --- | --- |
| `clientId` | Required; must be the ID registered on this socket. |
| `latitude` | Required number in `[-90, 90]`. |
| `longitude` | Required number in `[-180, 180]`. |
| `timestamp` | Optional ISO-8601 device timestamp. |

When the connection closes, the latest location remains cached but is changed to `isOnline: false`, subscribed admins receive that update, and the client's active tracking session is stopped.

#### `client.subscribers` (server → client)

Sent after registration and whenever an admin starts or stops subscribing to this client.

```json
{"type":"client.subscribers","subscribersCount":2}
```

### Admin messages

#### `admin.register`

Registers the socket as an admin dashboard.

```json
{"type":"admin.register"}
```

**Server response**

```json
{"type":"admin.registered"}
```

#### `admin.subscribe`

Subscribes this admin socket to one or more clients. The server acknowledges the subscription and immediately sends the latest cached location for each requested client that has one. It then sends future updates for those clients only to this admin socket.

```json
{
  "type": "admin.subscribe",
  "clientIds": ["device-123", "device-456"]
}
```

**Server acknowledgement**

```json
{"type":"admin.subscribed","clientIds":["device-123","device-456"]}
```

`clientIds` must be a non-empty array. Register as admin first.

#### `admin.unsubscribe`

Stops this admin socket from receiving updates for the given clients.

```json
{
  "type": "admin.unsubscribe",
  "clientIds": ["device-456"]
}
```

**Server acknowledgement**

```json
{"type":"admin.unsubscribed","clientIds":["device-456"]}
```

#### `admin.start_tracking`

Starts a new persistent tracking session for a client. Any active session for that client is ended first. If a latest location is cached, it becomes the session's first saved point.

```json
{
  "type": "admin.start_tracking",
  "clientId": "device-123",
  "sessionTag": "manual-review"
}
```

**Server response**

```json
{
  "type": "admin.tracking_started",
  "clientId": "device-123",
  "message": "Session ID: 01234567-89AB-4CDE-8F01-23456789ABCD"
}
```

#### `admin.stop_tracking`

Stops a client's active persistent tracking session. If a latest location is cached, it is saved as the session's final point.

```json
{
  "type": "admin.stop_tracking",
  "clientId": "device-123"
}
```

**Server response**

```json
{
  "type": "admin.tracking_stopped",
  "clientId": "device-123",
  "message": "Stopped Session: 01234567-89AB-4CDE-8F01-23456789ABCD"
}
```

### Location updates (server → admin)

An admin receives this message after subscribing to an already-known client, whenever that client publishes a location, and when the client goes offline:

```json
{
  "type": "location.update",
  "clientId": "device-123",
  "location": {
    "clientId": "device-123",
    "latitude": 35.6892,
    "longitude": 51.3890,
    "timestamp": "2026-08-01T12:00:00Z",
    "receivedAt": "2026-08-01T12:00:01Z",
    "isOnline": true
  }
}
```

`receivedAt` is the server's ISO-8601 receipt time. `timestamp` is the optional value supplied by the device. The same last coordinates are sent with `isOnline: false` on WebSocket disconnect, explicit REST stop, or REST heartbeat expiry.

### Error messages

Malformed JSON, unknown message types, missing fields, invalid WebSocket coordinates, wrong registration order, and role conflicts produce a message in this form:

```json
{"type":"error","message":"A human-readable explanation."}
```

## Tracking and persistence

The application uses three SQLite tables:

| Table | Contents |
| --- | --- |
| `clients` | Persistent device ID, optional name, and creation time. |
| `tracking_sessions` | Client reference, optional session tag, start/end times, and accumulated `totalDistanceKm`. |
| `location_points` | Saved latitude/longitude breadcrumbs with optional device timestamp and server receipt time. |

For an active session, the first position is saved immediately. Further positions are saved only when the device has moved more than 5 metres or more than 60 seconds have passed since the prior saved point. This reduces database churn. Distance is calculated between saved points with the Haversine formula and accumulated in kilometres. Ending a session saves the latest cached position once more as its final breadcrumb.

## Docker

The supplied Compose configuration builds the image and starts Vapor on port 8080 inside the container:

```bash
docker compose up --build app
```

Its defaults are deployment-specific:

- It joins an external Docker network named `n8n_default` (declared as `caddy_net`). Create that network or adapt `docker-compose.yml`.
- It mounts `/home/afshar/maps`, `/home/afshar/fonts`, and `/home/afshar/sprites` as read-only paths inside the container. Change these host paths for your machine.
- It does not publish port `8080` to the host; add a `ports` mapping if a reverse proxy is not providing access.
- The map archive must be copied into the image's working directory or otherwise mounted there with the expected filename. The provided Compose mounts map assets at `/maps`, while the current server looks for the MBTiles file in its working directory, so adapt either the mount or the application path before relying on map tile routes in Docker.
- `db.sqlite` is created in `/app` in the container. Mount a persistent volume there if history must survive container replacement.

## Security and deployment

The current service has **no authentication or authorization**. Any caller that can reach it can publish locations, read tracking history, load map assets, subscribe as an admin, or start and stop tracking sessions. Place it behind a trusted reverse proxy and implement authentication and role checks before exposing it publicly.

For production, use TLS (`https`/`wss`), restrict network access, add rate limiting and request-size limits, validate REST coordinates server-side, protect history endpoints, and persist the application database on durable storage. If running behind a proxy, forward the correct `Host` and `X-Forwarded-Proto` headers so the generated map-style URLs are public and use the right scheme.

## Technology

- [Vapor](https://vapor.codes/)
- [Fluent](https://docs.vapor.codes/fluent/overview/)
- [Fluent SQLite Driver](https://github.com/vapor/fluent-sqlite-driver)
