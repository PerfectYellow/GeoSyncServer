import NIOConcurrencyHelpers
import Vapor
import Fluent
import FluentSQLiteDriver

/// The JSON object sent by a client when publishing its position.
struct LocationPayload: Content {
    let latitude: Double
    let longitude: Double
    /// An optional ISO-8601 timestamp supplied by the device.
    let timestamp: String?
}

/// One WebSocket message shape used by both mobile roles.
///
/// `type` determines which optional fields are required. Keeping a single envelope makes
/// the protocol straightforward to use from Swift, Kotlin, JavaScript, and other clients.
struct LiveLocationMessage: Content {
    let type: String
    let clientId: String?
    let clientIds: [String]?
    let latitude: Double?
    let longitude: Double?
    let timestamp: String?
    let message: String?
}

private struct StoredLocation: Content {
    let clientId: String
    let latitude: Double
    let longitude: Double
    let timestamp: String?
    let receivedAt: String
    let isOnline: Bool
}

private struct ServerEvent: Content {
    let type: String
    let clientId: String?
    let clientIds: [String]?
    let location: StoredLocation?
    let message: String?
    let subscribersCount: Int?

    init(
        type: String,
        clientId: String? = nil,
        clientIds: [String]? = nil,
        location: StoredLocation? = nil,
        message: String? = nil,
        subscribersCount: Int? = nil
    ) {
        self.type = type
        self.clientId = clientId
        self.clientIds = clientIds
        self.location = location
        self.message = message
        self.subscribersCount = subscribersCount
    }
}

/// Thread-safe, process-local relay state. It deliberately has no persistence: restarting
/// the process clears locations and subscriptions.
private final class LiveLocationHub: @unchecked Sendable {
    private struct AdminConnection {
        let socket: WebSocket
        var subscriptions: Set<String>
    }

    private struct State {
        var clientByConnection: [UUID: String] = [:]
        var clientSockets: [UUID: WebSocket] = [:]
        var admins: [UUID: AdminConnection] = [:]
        var latestLocations: [String: StoredLocation] = [:]
    }

    private let state = NIOLockedValueBox(State())

    func registerClient(connectionId: UUID, clientId: String, socket: WebSocket) -> (Bool, [(WebSocket, ServerEvent)]) {
        self.state.withLockedValue { state in
            guard state.admins[connectionId] == nil, state.clientByConnection[connectionId] == nil else { return (false, []) }
            state.clientByConnection[connectionId] = clientId
            state.clientSockets[connectionId] = socket
            
            // Initial subscriber count for the new client
            let subscribersCount = state.admins.values.filter { $0.subscriptions.contains(clientId) }.count
            let registrationEvent = (socket, ServerEvent(type: "client.subscribers", subscribersCount: subscribersCount))

            // If we have a previous location, update it to online
            var events: [(WebSocket, ServerEvent)] = [registrationEvent]
            if var location = state.latestLocations[clientId] {
                location = StoredLocation(
                    clientId: clientId,
                    latitude: location.latitude,
                    longitude: location.longitude,
                    timestamp: location.timestamp,
                    receivedAt: location.receivedAt,
                    isOnline: true
                )
                state.latestLocations[clientId] = location
                let event = ServerEvent(type: "location.update", clientId: clientId, location: location)
                events.append(contentsOf: state.admins.values
                    .filter { $0.subscriptions.contains(clientId) }
                    .map { ($0.socket, event) })
            }
            return (true, events)
        }
    }

    func registerAdmin(connectionId: UUID, socket: WebSocket) -> Bool {
        self.state.withLockedValue { state in
            guard state.clientByConnection[connectionId] == nil, state.admins[connectionId] == nil else { return false }
            state.admins[connectionId] = AdminConnection(socket: socket, subscriptions: [])
            return true
        }
    }

    func subscribe(connectionId: UUID, clientIds: [String]) -> ([StoredLocation], [(WebSocket, ServerEvent)])? {
        self.state.withLockedValue { state in
            guard var admin = state.admins[connectionId] else { return nil }
            let newClientIds = Set(clientIds).subtracting(admin.subscriptions)
            admin.subscriptions.formUnion(clientIds)
            state.admins[connectionId] = admin
            
            let locations = clientIds.compactMap { state.latestLocations[$0] }
            
            // Notify clients about new subscribers
            let notifications = newClientIds.compactMap { clientId -> (WebSocket, ServerEvent)? in
                guard let connId = state.clientByConnection.first(where: { $0.value == clientId })?.key,
                      let socket = state.clientSockets[connId] else { return nil }
                let count = state.admins.values.filter { $0.subscriptions.contains(clientId) }.count
                return (socket, ServerEvent(type: "client.subscribers", subscribersCount: count))
            }
            
            return (locations, notifications)
        }
    }

    func unsubscribe(connectionId: UUID, clientIds: [String]) -> [(WebSocket, ServerEvent)]? {
        self.state.withLockedValue { state in
            guard var admin = state.admins[connectionId] else { return nil }
            let removedClientIds = Set(clientIds).intersection(admin.subscriptions)
            admin.subscriptions.subtract(clientIds)
            state.admins[connectionId] = admin
            
            // Notify clients about unsubscribed admin
            let notifications = removedClientIds.compactMap { clientId -> (WebSocket, ServerEvent)? in
                guard let connId = state.clientByConnection.first(where: { $0.value == clientId })?.key,
                      let socket = state.clientSockets[connId] else { return nil }
                let count = state.admins.values.filter { $0.subscriptions.contains(clientId) }.count
                return (socket, ServerEvent(type: "client.subscribers", subscribersCount: count))
            }
            
            return notifications
        }
    }

    func publish(connectionId: UUID, clientId: String, payload: LocationPayload) -> [(WebSocket, ServerEvent)]? {
        self.state.withLockedValue { state in
            guard state.clientByConnection[connectionId] == clientId else { return nil }

            let location = StoredLocation(
                clientId: clientId,
                latitude: payload.latitude,
                longitude: payload.longitude,
                timestamp: payload.timestamp,
                receivedAt: ISO8601DateFormatter().string(from: Date()),
                isOnline: true
            )
            state.latestLocations[clientId] = location
            let event = ServerEvent(type: "location.update", clientId: clientId, location: location)
            
            return state.admins.values
                .filter { $0.subscriptions.contains(clientId) }
                .map { ($0.socket, event) }
        }
    }

    func remove(connectionId: UUID) -> [(WebSocket, ServerEvent)] {
        self.state.withLockedValue { state in
            var events: [(WebSocket, ServerEvent)] = []
            
            if let clientId = state.clientByConnection.removeValue(forKey: connectionId) {
                state.clientSockets.removeValue(forKey: connectionId)
                // When client disconnects, mark as offline but KEEP the location
                if var location = state.latestLocations[clientId] {
                    location = StoredLocation(
                        clientId: clientId,
                        latitude: location.latitude,
                        longitude: location.longitude,
                        timestamp: location.timestamp,
                        receivedAt: location.receivedAt,
                        isOnline: false
                    )
                    state.latestLocations[clientId] = location
                    let event = ServerEvent(type: "location.update", clientId: clientId, location: location)
                    events = state.admins.values
                        .filter { $0.subscriptions.contains(clientId) }
                        .map { ($0.socket, event) }
                }
            }
            
            if let admin = state.admins.removeValue(forKey: connectionId) {
                // Notify clients that this admin was watching
                for clientId in admin.subscriptions {
                    if let connId = state.clientByConnection.first(where: { $0.value == clientId })?.key,
                       let socket = state.clientSockets[connId] {
                        let count = state.admins.values.filter { $0.subscriptions.contains(clientId) }.count
                        events.append((socket, ServerEvent(type: "client.subscribers", subscribersCount: count)))
                    }
                }
            }
            
            return events
        }
    }
}

private let jsonEncoder = JSONEncoder()
private let jsonDecoder = JSONDecoder()

private func send(_ event: ServerEvent, to socket: WebSocket) {
    do {
        let data = try jsonEncoder.encode(event)
        guard let text = String(data: data, encoding: .utf8) else {
            throw Abort(.internalServerError)
        }
        socket.send(text)
    } catch {
        socket.send(#"{"type":"error","message":"Unable to encode server response."}"#)
    }
}

extension DatabaseID {
    static let mbtiles = DatabaseID(string: "mbtiles")
}

func routes(_ app: Application) throws {
    let hub = LiveLocationHub()

    // --- MBTiles Setup ---
    let mbtilesPath = "osm-2020-02-10-v3.11_iran_tehran.mbtiles"
    let fileManager = FileManager.default
    if fileManager.fileExists(atPath: mbtilesPath) {
        print("✅ MBTiles file found at: \(mbtilesPath)")
    } else {
        print("❌ MBTiles file NOT found at: \(mbtilesPath). Currently in: \(fileManager.currentDirectoryPath)")
    }
    app.databases.use(.sqlite(.file(mbtilesPath)), as: .mbtiles)

    app.get { _ in
        "GeoSync live-location relay is running. Connect with WebSocket at /v1/live."
    }

    app.get("health") { _ in
        ["status": "ok"]
    }

    // --- Internal Map Tile Server ---
    app.get("v1", "map", "tiles", ":z", ":x", ":y") { req -> EventLoopFuture<Response> in
        guard let z = req.parameters.get("z", as: Int.self),
              let x = req.parameters.get("x", as: Int.self),
              let y = req.parameters.get("y", as: Int.self) else {
            return req.eventLoop.makeFailedFuture(Abort(.badRequest))
        }

        // MBTiles uses TMS (Tile Map Service) coordinates, so we must flip the Y axis
        let y_tms = Int(pow(2.0, Double(z))) - 1 - y

        let db = req.db(.mbtiles) as! (any SQLiteDatabase)
        let query = "SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?"
        
        return db.query(query, [
            SQLiteData.integer(z),
            SQLiteData.integer(x),
            SQLiteData.integer(y_tms)
        ]).map { rows in
            guard let row = rows.first,
                  let tileData = row.column("tile_data")?.blob else {
                return Response(status: .notFound)
            }
            
            let response = Response(status: .ok, body: .init(buffer: tileData))
            // Your metadata says 'format: pbf', which means Vector Tiles.
            response.headers.replaceOrAdd(name: .contentType, value: "application/x-protobuf")
            // Vector tiles in MBTiles are often gzipped. MapLibre expects this.
            response.headers.replaceOrAdd(name: .contentEncoding, value: "gzip")
            return response
        }
    }

    // Simple Mapbox Style for Internal Vector Tiles
    app.get("v1", "map", "style.json") { req -> Response in
        let host = req.headers.first(name: .host) ?? "localhost:8080"
        let scheme = req.application.http.server.configuration.tlsConfiguration == nil ? "http" : "https"
        
        let style = """
        {
          "version": 8,
          "name": "GeoSync Internal",
          "sources": {
            "internal": {
              "type": "vector",
              "tiles": ["\(scheme)://\(host)/v1/map/tiles/{z}/{x}/{y}"],
              "minzoom": 0,
              "maxzoom": 14
            }
          },
          "layers": [
            {
              "id": "background",
              "type": "background",
              "paint": { "background-color": "#f8f4f0" }
            },
            {
              "id": "water",
              "source": "internal",
              "source-layer": "water",
              "type": "fill",
              "paint": { "fill-color": "#a0cfdf" }
            },
            {
              "id": "roads",
              "source": "internal",
              "source-layer": "transportation",
              "type": "line",
              "paint": { "line-color": "#ffffff", "line-width": 1 }
            },
            {
              "id": "buildings",
              "source": "internal",
              "source-layer": "building",
              "type": "fill",
              "paint": { "fill-color": "#dcdcdc" }
            }
          ]
        }
        """
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        return Response(status: .ok, headers: headers, body: .init(string: style))
    }

    // Diagnostic route to check map metadata
    app.get("v1", "map", "metadata") { req -> EventLoopFuture<[String: String]> in
        let db = req.db(.mbtiles) as! (any SQLiteDatabase)
        return db.query("SELECT name, value FROM metadata").map { rows in
            var metadata: [String: String] = [:]
            for row in rows {
                if let name = row.column("name")?.string, let value = row.column("value")?.string {
                    metadata[name] = value
                }
            }
            return metadata
        }
    }

    app.webSocket("v1", "live") { _, socket in
        let connectionId = UUID()
        send(ServerEvent(type: "connected", message: "Register as client or admin before sending other messages."), to: socket)

        socket.onClose.whenComplete { _ in
            let events = hub.remove(connectionId: connectionId)
            events.forEach { send($0.1, to: $0.0) }
        }

        socket.onText { socket, text in
            let incoming: LiveLocationMessage
            do {
                incoming = try jsonDecoder.decode(LiveLocationMessage.self, from: Data(text.utf8))
            } catch {
                send(ServerEvent(type: "error", message: "Invalid JSON message."), to: socket)
                return
            }

            switch incoming.type {
            case "client.register":
                guard let clientId = incoming.clientId else {
                    send(ServerEvent(type: "error", message: "clientId is required."), to: socket)
                    return
                }
                let (success, events) = hub.registerClient(connectionId: connectionId, clientId: clientId, socket: socket)
                guard success else {
                    send(ServerEvent(type: "error", message: "This socket already has a different role."), to: socket)
                    return
                }
                send(ServerEvent(type: "client.registered", clientId: clientId), to: socket)
                events.forEach { send($0.1, to: $0.0) }

            case "client.location":
                guard let clientId = incoming.clientId,
                      let latitude = incoming.latitude,
                      let longitude = incoming.longitude,
                      (-90...90).contains(latitude),
                      (-180...180).contains(longitude)
                else {
                    send(ServerEvent(type: "error", message: "clientId, valid latitude, and valid longitude are required."), to: socket)
                    return
                }

                let payload = LocationPayload(latitude: latitude, longitude: longitude, timestamp: incoming.timestamp)
                guard let events = hub.publish(connectionId: connectionId, clientId: clientId, payload: payload) else {
                    send(ServerEvent(type: "error", message: "Register this clientId on this socket before publishing."), to: socket)
                    return
                }
                events.forEach { send($0.1, to: $0.0) }

            case "admin.register":
                guard hub.registerAdmin(connectionId: connectionId, socket: socket) else {
                    send(ServerEvent(type: "error", message: "This socket already has a different role."), to: socket)
                    return
                }
                send(ServerEvent(type: "admin.registered"), to: socket)

            case "admin.subscribe":
                guard let clientIds = incoming.clientIds, !clientIds.isEmpty else {
                    send(ServerEvent(type: "error", message: "clientIds must contain at least one ID."), to: socket)
                    return
                }
                guard let result = hub.subscribe(connectionId: connectionId, clientIds: clientIds) else {
                    send(ServerEvent(type: "error", message: "Register as admin before subscribing."), to: socket)
                    return
                }
                let (cachedLocations, notifications) = result
                send(ServerEvent(type: "admin.subscribed", clientIds: clientIds), to: socket)
                cachedLocations.forEach { location in
                    send(ServerEvent(type: "location.update", clientId: location.clientId, location: location), to: socket)
                }
                notifications.forEach { send($0.1, to: $0.0) }

            case "admin.unsubscribe":
                guard let clientIds = incoming.clientIds, !clientIds.isEmpty else {
                    send(ServerEvent(type: "error", message: "clientIds must contain at least one ID."), to: socket)
                    return
                }
                guard let notifications = hub.unsubscribe(connectionId: connectionId, clientIds: clientIds) else {
                    send(ServerEvent(type: "error", message: "Register as admin before unsubscribing."), to: socket)
                    return
                }
                send(ServerEvent(type: "admin.unsubscribed", clientIds: clientIds), to: socket)
                notifications.forEach { send($0.1, to: $0.0) }

            default:
                send(ServerEvent(type: "error", message: "Unknown message type: \(incoming.type)."), to: socket)
            }
        }
    }
}
