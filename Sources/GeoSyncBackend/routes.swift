import NIOConcurrencyHelpers
import Vapor

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
    let clientId: UUID?
    let clientIds: [UUID]?
    let latitude: Double?
    let longitude: Double?
    let timestamp: String?
    let message: String?
}

private struct StoredLocation: Content {
    let clientId: UUID
    let latitude: Double
    let longitude: Double
    let timestamp: String?
    let receivedAt: String
    let isOnline: Bool
}

private struct ServerEvent: Content {
    let type: String
    let clientId: UUID?
    let clientIds: [UUID]?
    let location: StoredLocation?
    let message: String?
    let subscribersCount: Int?

    init(
        type: String,
        clientId: UUID? = nil,
        clientIds: [UUID]? = nil,
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
        var subscriptions: Set<UUID>
    }

    private struct State {
        var clientByConnection: [UUID: UUID] = [:]
        var clientSockets: [UUID: WebSocket] = [:]
        var admins: [UUID: AdminConnection] = [:]
        var latestLocations: [UUID: StoredLocation] = [:]
    }

    private let state = NIOLockedValueBox(State())

    func registerClient(connectionId: UUID, clientId: UUID, socket: WebSocket) -> (Bool, [(WebSocket, ServerEvent)]) {
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

    func subscribe(connectionId: UUID, clientIds: [UUID]) -> ([StoredLocation], [(WebSocket, ServerEvent)])? {
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

    func unsubscribe(connectionId: UUID, clientIds: [UUID]) -> [(WebSocket, ServerEvent)]? {
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

    func publish(connectionId: UUID, clientId: UUID, payload: LocationPayload) -> [(WebSocket, ServerEvent)]? {
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

func routes(_ app: Application) throws {
    let hub = LiveLocationHub()

    app.get { _ in
        "GeoSync live-location relay is running. Connect with WebSocket at /v1/live."
    }

    app.get("health") { _ in
        ["status": "ok"]
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
                    send(ServerEvent(type: "error", message: "clientIds must contain at least one UUID."), to: socket)
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
                    send(ServerEvent(type: "error", message: "clientIds must contain at least one UUID."), to: socket)
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
