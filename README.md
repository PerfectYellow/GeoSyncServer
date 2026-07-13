# GeoSyncBackend

An in-memory, WebSocket live-location relay built with Vapor 4. It deliberately
does not use a database: locations and subscriptions disappear when the server
restarts or their socket closes.

## API

`GET /health` returns `{ "status": "ok" }`.

Connect clients and admins to `ws://<host>:<port>/v1/live`. Each WebSocket text
frame is JSON. UUID values must be canonical UUID strings.

Client flow:

```json
{"type":"client.register","clientId":"1B6BEB81-8A98-4DEC-8BE9-C224FEB760F2"}
{"type":"client.location","clientId":"1B6BEB81-8A98-4DEC-8BE9-C224FEB760F2","latitude":35.6892,"longitude":51.3890,"timestamp":"2026-07-13T12:00:00Z"}
```

Generate the `clientId` once in the client app and show it to the user. The
client must register before publishing and may publish only its registered ID.

Admin flow (one socket can track any number of clients):

```json
{"type":"admin.register"}
{"type":"admin.subscribe","clientIds":["1B6BEB81-8A98-4DEC-8BE9-C224FEB760F2"]}
{"type":"admin.unsubscribe","clientIds":["1B6BEB81-8A98-4DEC-8BE9-C224FEB760F2"]}
```

After `admin.subscribe`, the server immediately sends the latest known location
when one exists, then sends future updates only to that admin socket:

```json
{
  "type":"location.update",
  "clientId":"1B6BEB81-8A98-4DEC-8BE9-C224FEB760F2",
  "location":{
    "clientId":"1B6BEB81-8A98-4DEC-8BE9-C224FEB760F2",
    "latitude":35.6892,
    "longitude":51.3890,
    "timestamp":"2026-07-13T12:00:00Z",
    "receivedAt":"2026-07-13T12:00:01Z"
  }
}
```

The relay has no authentication. For a real deployment, put authenticated
client/admin identity and authorization in front of this endpoint, use WSS, and
consider rate limits and coordinate validation appropriate to your product.

## Getting Started

To build the project using the Swift Package Manager, run the following command in the terminal from the root of the project:
```bash
swift build
```

To run the project and start the server, use the following command:
```bash
swift run
```

To execute tests, use the following command:
```bash
swift test
```

### See more

- [Vapor Website](https://vapor.codes)
- [Vapor Documentation](https://docs.vapor.codes)
- [Vapor GitHub](https://github.com/vapor)
- [Vapor Community maintained packages](https://github.com/vapor-community)
