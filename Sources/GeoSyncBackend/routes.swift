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
    /// If true, bypasses throttling and saves the point immediately.
    var isManual: Bool? = false
}

/// One WebSocket message shape used by both mobile roles.
struct LiveLocationMessage: Content {
    let type: String
    let clientId: String?
    let clientIds: [String]?
    let latitude: Double?
    let longitude: Double?
    let timestamp: String?
    let message: String?
    let sessionTag: String?
    let isManual: Bool?
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

// MARK: - History query parameters

/// Query string shape for GET /v1/history and /v1/history/:clientId
/// Example: /v1/history/abc123?from=2025-01-01T00:00:00Z&to=2025-01-02T00:00:00Z&page=1&per=50
private struct HistoryQuery: Content {
    /// ISO-8601 start of the requested window (inclusive). If nil, no lower bound.
    let from: String?
    /// ISO-8601 end of the requested window (inclusive). If nil, no upper bound.
    let to: String?
    /// Page number, 1-indexed. Defaults to 1.
    let page: Int?
    /// Items per page. Defaults to 50, capped at 200 to keep payloads sane.
    let per: Int?
}

/// A page of tracking sessions with metadata about total count / pages remaining,
/// so the client's lazy-loading UI knows when to stop asking.
private struct HistoryPage: Content {
    let sessions: [TrackingSession]
    let page: Int
    let per: Int
    let total: Int
    let hasMore: Bool
}

/// Thread-safe, process-local relay state.
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

        /// Maps clientId to an active TrackingSession ID in the database.
        var activeSessions: [String: UUID] = [:]

        /// Tracks the last saved point for throttling (to avoid DB jitter).
        var lastSavedPoints: [String: (latitude: Double, longitude: Double, time: Date)] = [:]

        /// Tracks last update time for REST clients to detect offline status.
        var lastSeenREST: [String: Date] = [:]
    }

    private let state = NIOLockedValueBox(State())
    private let db: any Database

    init(db: any Database) {
        self.db = db
    }

    func registerClient(connectionId: UUID, clientId: String, socket: WebSocket) -> (Bool, [(WebSocket, ServerEvent)]) {
        // ... (existing logic)
        self.state.withLockedValue { state in
            guard state.admins[connectionId] == nil, state.clientByConnection[connectionId] == nil else { return (false, []) }
            state.clientByConnection[connectionId] = clientId
            state.clientSockets[connectionId] = socket

            let subscribersCount = state.admins.values.filter { $0.subscriptions.contains(clientId) }.count
            let registrationEvent = (socket, ServerEvent(type: "client.subscribers", subscribersCount: subscribersCount))

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
        let now = Date()
        let result = self.state.withLockedValue { state -> (UUID?, (latitude: Double, longitude: Double, time: Date)?, [(WebSocket, ServerEvent)]?) in
            guard state.clientByConnection[connectionId] == clientId else { return (nil, nil, nil) }

            let location = StoredLocation(
                clientId: clientId,
                latitude: payload.latitude,
                longitude: payload.longitude,
                timestamp: payload.timestamp,
                receivedAt: ISO8601DateFormatter().string(from: now),
                isOnline: true
            )
            state.latestLocations[clientId] = location
            let event = ServerEvent(type: "location.update", clientId: clientId, location: location)

            let adminEvents = state.admins.values
                .filter { $0.subscriptions.contains(clientId) }
                .map { ($0.socket, event) }

            state.lastSeenREST[clientId] = now

            return (state.activeSessions[clientId], state.lastSavedPoints[clientId], adminEvents)
        }

        guard let adminEvents = result.2 else { return nil }

        // --- Persistence Logic (Senior Approach: Throttling & Incremental Stats) ---
        if let sessionId = result.0 {
            let lastPoint = result.1
            let currentLat = payload.latitude
            let currentLon = payload.longitude

            var shouldSave = false
            var distanceIncrement: Double = 0.0

            if payload.isManual == true {
                shouldSave = true
                if let last = lastPoint {
                    distanceIncrement = haversineDistance(lat1: last.latitude, lon1: last.longitude, lat2: currentLat, lon2: currentLon)
                }
            } else if let last = lastPoint {
                let dist = haversineDistance(lat1: last.latitude, lon1: last.longitude, lat2: currentLat, lon2: currentLon)
                let timeSince = now.timeIntervalSince(last.time)

                // Only save if moved > 5 meters OR > 60 seconds passed (heartbeat)
                if dist > 0.005 || timeSince > 60 {
                    shouldSave = true
                    distanceIncrement = dist
                }
            } else {
                // First point in session
                shouldSave = true
            }

            if shouldSave {
                self.state.withLockedValue { $0.lastSavedPoints[clientId] = (currentLat, currentLon, now) }

                Task {
                    do {
                        // Create point
                        let point = LocationPoint(
                            sessionId: sessionId,
                            latitude: currentLat,
                            longitude: currentLon,
                            timestamp: payload.timestamp?.iso8601Date
                        )
                        try await point.save(on: self.db)

                        // Update session stats
                        if let session = try await TrackingSession.find(sessionId, on: self.db) {
                            session.totalDistanceKm += distanceIncrement
                            session.endTime = now
                            try await session.update(on: self.db)
                        }
                    } catch {
                        print("❌ Failed to persist location: \(error)")
                    }
                }
            }
        }

        return adminEvents
    }

    func publishREST(clientId: String, payload: LocationPayload) async throws -> [(WebSocket, ServerEvent)] {
        let now = Date()
        let result = self.state.withLockedValue { state -> (UUID?, (latitude: Double, longitude: Double, time: Date)?, [(WebSocket, ServerEvent)]) in
            let location = StoredLocation(
                clientId: clientId,
                latitude: payload.latitude,
                longitude: payload.longitude,
                timestamp: payload.timestamp,
                receivedAt: ISO8601DateFormatter().string(from: now),
                isOnline: true
            )
            state.latestLocations[clientId] = location
            let event = ServerEvent(type: "location.update", clientId: clientId, location: location)

            let adminEvents = state.admins.values
                .filter { $0.subscriptions.contains(clientId) }
                .map { ($0.socket, event) }

            state.lastSeenREST[clientId] = now

            return (state.activeSessions[clientId], state.lastSavedPoints[clientId], adminEvents)
        }

        let adminEvents = result.2
        let sessionId: UUID
        if let existingId = result.0 {
            sessionId = existingId
        } else {
            sessionId = try await startTracking(clientId: clientId, sessionTag: "REST")
        }

        let lastPoint = result.1
        let currentLat = payload.latitude
        let currentLon = payload.longitude

        var shouldSave = false
        var distanceIncrement: Double = 0.0

        if payload.isManual == true {
            shouldSave = true
            if let last = lastPoint {
                distanceIncrement = haversineDistance(lat1: last.latitude, lon1: last.longitude, lat2: currentLat, lon2: currentLon)
            }
        } else if let last = lastPoint {
            let dist = haversineDistance(lat1: last.latitude, lon1: last.longitude, lat2: currentLat, lon2: currentLon)
            let timeSince = now.timeIntervalSince(last.time)
            if dist > 0.005 || timeSince > 60 {
                shouldSave = true
                distanceIncrement = dist
            }
        } else {
            shouldSave = true
        }

        if shouldSave {
            self.state.withLockedValue { $0.lastSavedPoints[clientId] = (currentLat, currentLon, now) }
            let point = LocationPoint(
                sessionId: sessionId,
                latitude: currentLat,
                longitude: currentLon,
                timestamp: payload.timestamp?.iso8601Date
            )
            try await point.save(on: self.db)
            if let session = try await TrackingSession.find(sessionId, on: self.db) {
                session.totalDistanceKm += distanceIncrement
                session.endTime = now
                try await session.update(on: self.db)
            }
        }
        return adminEvents
    }

    func cleanupRESTSessions() async {
        let now = Date()
        let timeout: TimeInterval = 60

        let expiredClientIds = self.state.withLockedValue { state -> [String] in
            state.lastSeenREST.filter { now.timeIntervalSince($0.value) > timeout }.map { $0.key }
        }

        for clientId in expiredClientIds {
            print("⏱️ REST Heartbeat timeout for \(clientId). Cleaning up...")

            // Mark as offline and notify admins
            let adminEvents = self.state.withLockedValue { state -> [(WebSocket, ServerEvent)] in
                state.lastSeenREST.removeValue(forKey: clientId)

                guard var location = state.latestLocations[clientId] else { return [] }
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
                return state.admins.values
                    .filter { $0.subscriptions.contains(clientId) }
                    .map { ($0.socket, event) }
            }

            // Notify admins
            adminEvents.forEach { send($0.1, to: $0.0) }

            // Close tracking session in DB
            do {
                if let sessionId = try await stopTracking(clientId: clientId) {
                    print("✅ Closed REST session \(sessionId) for \(clientId)")
                }
            } catch {
                print("❌ Failed to close REST session for \(clientId): \(error)")
            }
        }
    }

    func stopREST(clientId: String) async throws {
        print("🛑 REST explicit stop for \(clientId)")

        // Mark as offline and notify admins
        let adminEvents = self.state.withLockedValue { state -> [(WebSocket, ServerEvent)] in
            state.lastSeenREST.removeValue(forKey: clientId)

            guard var location = state.latestLocations[clientId] else {
                print("⚠️ No latest location for \(clientId) to mark offline.")
                return []
            }
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
            let admins = state.admins.values
                .filter { $0.subscriptions.contains(clientId) }

            print("📣 Notifying \(admins.count) admins about \(clientId) going offline.")
            return admins.map { ($0.socket, event) }
        }

        // Notify admins
        adminEvents.forEach { send($0.1, to: $0.0) }

        // Close tracking session in DB
        _ = try await stopTracking(clientId: clientId)
    }

    func startTracking(clientId: String, sessionTag: String?) async throws -> UUID {
        // Ensure client exists in DB
        if try await Client.find(clientId, on: self.db) == nil {
            try await Client(id: clientId).create(on: self.db)
        }

        // Stop any existing session
        _ = try await stopTracking(clientId: clientId)

        // Create new session
        let session = TrackingSession(clientId: clientId, sessionTag: sessionTag)
        try await session.create(on: self.db)
        let sessionId = try session.requireID()

        // --- NEW: Save initial location immediately if we have it ---
        let initialLocation = self.state.withLockedValue { state in
            state.activeSessions[clientId] = sessionId
            state.lastSavedPoints.removeValue(forKey: clientId)
            return state.latestLocations[clientId]
        }

        if let loc = initialLocation {
            let point = LocationPoint(
                sessionId: sessionId,
                latitude: loc.latitude,
                longitude: loc.longitude,
                timestamp: loc.timestamp?.iso8601Date
            )
            try await point.save(on: self.db)
            self.state.withLockedValue { $0.lastSavedPoints[clientId] = (loc.latitude, loc.longitude, Date()) }
        }

        return sessionId
    }

    func stopTracking(clientId: String) async throws -> UUID? {
        let sessionData = self.state.withLockedValue { state -> (UUID?, StoredLocation?) in
            let id = state.activeSessions.removeValue(forKey: clientId)
            let loc = state.latestLocations[clientId]
            return (id, loc)
        }

        guard let sessionId = sessionData.0 else { return nil }

        if let session = try await TrackingSession.find(sessionId, on: self.db) {
            // --- NEW: Save final location as the last breadcrumb ---
            if let loc = sessionData.1 {
                let point = LocationPoint(
                    sessionId: sessionId,
                    latitude: loc.latitude,
                    longitude: loc.longitude,
                    timestamp: loc.timestamp?.iso8601Date
                )
                try await point.save(on: self.db)
            }

            session.endTime = Date()
            try await session.update(on: self.db)
        }
        return sessionId
    }

    func remove(connectionId: UUID) -> (clientId: String?, events: [(WebSocket, ServerEvent)]) {
        self.state.withLockedValue { state in
            var events: [(WebSocket, ServerEvent)] = []
            var removedClientId: String? = nil

            if let clientId = state.clientByConnection.removeValue(forKey: connectionId) {
                removedClientId = clientId
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

            return (removedClientId, events)
        }
    }
}

/// Haversine formula to calculate distance between two points in km.
private func haversineDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double) -> Double {
    let R = 6371.0 // Earth radius in km
    let dLat = (lat2 - lat1) * .pi / 180.0
    let dLon = (lon2 - lon1) * .pi / 180.0
    let a = sin(dLat/2) * sin(dLat/2) +
            cos(lat1 * .pi / 180.0) * cos(lat2 * .pi / 180.0) *
            sin(dLon/2) * sin(dLon/2)
    let c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c
}

// MARK: - Date Formatting Helpers

extension String {
    var iso8601Date: Date? {
        let formatter = ISO8601DateFormatter()
        // Try with fractional seconds first (e.g. .567Z)
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = formatter.date(from: self) {
            return date
        }
        // Fallback to standard ISO8601
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: self)
    }
}

private func send(_ event: ServerEvent, to socket: WebSocket) {
    do {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(event)
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

private func fetchHistory(req: Request, clientId: String?) async throws -> HistoryPage {
    let query = try req.query.decode(HistoryQuery.self)

    var sessionQuery = TrackingSession.query(on: req.db)

    if let clientId {
        sessionQuery = sessionQuery.filter(\.$client.$id == clientId)
    }

    // A session is "in range" if it overlaps [from, to] at all:
    // session.startTime <= to  AND  (session.endTime >= from OR session.endTime is nil, i.e. still active)
    if let fromStr = query.from, let fromDate = fromStr.iso8601Date {
        sessionQuery = sessionQuery.group(.or) { group in
            group.filter(\.$endTime >= fromDate)
            group.filter(\.$endTime == nil)
        }
    }
    if let toStr = query.to, let toDate = toStr.iso8601Date {
        sessionQuery = sessionQuery.filter(\.$startTime <= toDate)
    }

    let page = query.page ?? 1
    let per = min(query.per ?? 50, 200)

    let total = try await sessionQuery.count()

    let sessions = try await sessionQuery
        .with(\.$points)
        .sort(\.$startTime, .descending)
        .range((page - 1) * per ..< page * per)
        .all()

    return HistoryPage(
        sessions: sessions,
        page: page,
        per: per,
        total: total,
        hasMore: page * per < total
    )
}

func routes(_ app: Application) throws {
    let hub = LiveLocationHub(db: app.db)

    // --- Background Cleanup Loop for REST Sessions ---
    Task {
        while true {
            try? await Task.sleep(nanoseconds: 30 * 1_000_000_000)
            await hub.cleanupRESTSessions()
        }
    }

    // --- MBTiles Setup ---
    // "/maps/osm-2020-02-10-v3.11_iran_tehran.mbtiles"
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

    // --- History: paginated, optionally time-bounded, across all clients ---
    app.get("v1", "history") { req async throws -> HistoryPage in
        try await fetchHistory(req: req, clientId: nil)
    }
    
    // --- History: paginated, optionally time-bounded, for one client ---
    app.get("v1", "history", ":clientId") { req async throws -> HistoryPage in
        guard let clientId = req.parameters.get("clientId") else {
            throw Abort(.badRequest)
        }
        return try await fetchHistory(req: req, clientId: clientId)
    }
    
    // --- Points only, for a session, restricted to a time window ---
    // Useful once you already know the sessionId (e.g. from the history list above)
    // and just want the breadcrumb trail for a sub-range of that session.
    app.get("v1", "history", "session", ":sessionId", "points") { req async throws -> Page<LocationPoint> in
        guard let sessionId = req.parameters.get("sessionId", as: UUID.self) else {
            throw Abort(.badRequest)
        }
        let query = try req.query.decode(HistoryQuery.self)
    
        var pointsQuery = LocationPoint.query(on: req.db)
            .filter(\.$session.$id == sessionId)
    
        if let fromStr = query.from, let fromDate = fromStr.iso8601Date {
            pointsQuery = pointsQuery.filter(\.$timestamp >= fromDate)
        }
        if let toStr = query.to, let toDate = toStr.iso8601Date {
            pointsQuery = pointsQuery.filter(\.$timestamp <= toDate)
        }
    
        let page = query.page ?? 1
        let per = min(query.per ?? 50, 200)
        return try await pointsQuery
            .sort(\.$timestamp, .ascending)
            .paginate(PageRequest(page: page, per: per))
    }

    // --- REST Location Update ---
    app.post("v1", "location", ":clientId") { req async throws -> String in
        guard let clientId = req.parameters.get("clientId") else {
            throw Abort(.badRequest)
        }
        let payload = try req.content.decode(LocationPayload.self)
        let adminEvents = try await hub.publishREST(clientId: clientId, payload: payload)

        // Notify admins
        adminEvents.forEach { send($0.1, to: $0.0) }

        return "ok"
    }

    app.post("v1", "location", ":clientId", "stop") { req async throws -> String in
        guard let clientId = req.parameters.get("clientId") else {
            throw Abort(.badRequest)
        }
        try await hub.stopREST(clientId: clientId)
        return "ok"
    }

    // // --- Diagnostic: Check Tracking History ---
    // app.get("v1", "history") { req async throws -> [TrackingSession] in
    //     try await TrackingSession.query(on: req.db)
    //         .with(\.$points)
    //         .sort(\.$startTime, .descending)
    //         .limit(10)
    //         .all()
    // }

    // // --- Client-Specific History ---
    // app.get("v1", "history", ":clientId") { req async throws -> [TrackingSession] in
    //     guard let clientId = req.parameters.get("clientId") else {
    //         throw Abort(.badRequest)
    //     }
    //     return try await TrackingSession.query(on: req.db)
    //         .filter(\.$client.$id == clientId)
    //         .with(\.$points)
    //         .sort(\.$startTime, .descending)
    //         .limit(20)
    //         .all()
    // }
    // 
    // // // --- All location points for a client across all sessions, in a time window ---
    // // GET /v1/history/:clientId/points?from=...&to=...&page=1&per=20
    // app.get("v1", "history", ":clientId", "points") { req async throws -> Page<LocationPoint> in
    //     guard let clientId = req.parameters.get("clientId") else {
    //         throw Abort(.badRequest)
    //     }
    //     let query = try req.query.decode(HistoryQuery.self)
    //     let iso = ISO8601DateFormatter()
    
    //     var pointsQuery = LocationPoint.query(on: req.db)
    //         .join(TrackingSession.self, on: \LocationPoint.$session.$id == \TrackingSession.$id)
    //         .filter(TrackingSession.self, \.$client.$id == clientId)
    
    //     if let fromStr = query.from, let fromDate = iso.date(from: fromStr) {
    //         pointsQuery = pointsQuery.filter(\.$timestamp >= fromDate)
    //     }
    //     if let toStr = query.to, let toDate = iso.date(from: toStr) {
    //         pointsQuery = pointsQuery.filter(\.$timestamp <= toDate)
    //     }
    
    //     let page = query.page ?? 1
    //     let per = min(query.per ?? 20, 200)
    //     return try await pointsQuery
    //         .sort(\.$timestamp, .ascending)
    //         .paginate(PageRequest(page: page, per: per))
    // }

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

        // Detect scheme robustly, especially when behind a proxy like Nginx or Cloudflare
        var scheme = "http"
        if let forwardedProto = req.headers.first(name: "X-Forwarded-Proto") {
            scheme = forwardedProto
        } else if req.application.http.server.configuration.tlsConfiguration != nil {
            scheme = "https"
        }

        // Updated style.json with labels and icons support.
        // NOTE: real JSON has no comments — keep any notes outside the string literal,
        // a stray "//" inside this triple-quoted string breaks parsing on the client.
        let style = """
        {
          "version": 8,
          "name": "GeoSync Internal",
          "glyphs": "\(scheme)://\(host)/fonts/{fontstack}/{range}.pbf",
          "sprite": "\(scheme)://\(host)/sprites/sprite",
          "sources": {
            "internal": {
              "type": "vector",
              "tiles": ["\(scheme)://\(host)/v1/map/tiles/{z}/{x}/{y}"],
              "minzoom": 0,
              "maxzoom": 14
            }
          },
          "layers": [
            { "id": "background", "type": "background", "paint": { "background-color": "#f8f4f0" } },
            { "id": "water", "source": "internal", "source-layer": "water", "type": "fill", "paint": { "fill-color": "#a0cfdf" } },
            { "id": "roads", "source": "internal", "source-layer": "transportation", "type": "line", "paint": { "line-color": "#ffffff", "line-width": 2 } },
            { "id": "buildings", "source": "internal", "source-layer": "building", "type": "fill", "paint": { "fill-color": "#dcdcdc" } },
            {
              "id": "place-labels",
              "source": "internal",
              "source-layer": "place",
              "type": "symbol",
              "layout": {
                "text-field": ["get", "name"],
                "text-font": ["Open Sans Regular"],
                "text-size": ["interpolate", ["linear"], ["zoom"], 4, 10, 12, 16]
              },
              "paint": {
                "text-color": "#333333",
                "text-halo-color": "#ffffff",
                "text-halo-width": 1.2
              }
            },
            {
              "id": "poi-icons",
              "source": "internal",
              "source-layer": "poi",
              "type": "symbol",
              "layout": {
                "icon-image": ["get", "class"],
                "icon-size": 0.8,
                "text-field": ["get", "name"],
                "text-font": ["Open Sans Regular"],
                "text-size": 11,
                "text-offset": [0, 1.2],
                "text-anchor": "top"
              },
              "paint": {
                "text-color": "#333333",
                "text-halo-color": "#ffffff",
                "text-halo-width": 1.0
              }
            }
          ]
        }
        """
        var headers = HTTPHeaders()
        headers.add(name: .contentType, value: "application/json")
        return Response(status: .ok, headers: headers, body: .init(string: style))
    }

    // --- Glyphs (required for any text-field / symbol layer to render) ---
    // Real files live at /fonts/_output/<fontstack>/<range> inside the container
    // (confirmed via `docker exec geosync-server ls /fonts/_output`), built by
    // running `npm install && node generate.js` in the openmaptiles/fonts clone.
    // Route path is root-level "fonts/..." to match the glyphs URL in style.json above.
    app.get("fonts", ":fontstack", ":range") { req async throws -> Response in
        guard let fontstack = req.parameters.get("fontstack"),
              let range = req.parameters.get("range") else {
            return Response(status: .badRequest)
        }
        let path = "/fonts/_output/\(fontstack)/\(range)"
        guard FileManager.default.fileExists(atPath: path) else {
            return Response(status: .notFound)
        }
        return try await req.fileio.asyncStreamFile(
            at: path,
            mediaType: .init(type: "application", subType: "x-protobuf")
        )
    }

    // --- Sprite (required for any icon-image to render) ---
    // Real files live at /sprites/sprite.json, /sprites/sprite.png, /sprites/sprite@2x.json,
    // /sprites/sprite@2x.png inside the container (confirmed via `docker exec ... ls /sprites`).
    // MapLibre appends these suffixes directly onto the "sprite" URL from style.json above —
    // it does NOT request a sub-path, so each filename needs its own literal route.
    @Sendable func serveSpriteFile(_ req: Request, filename: String, mediaType: HTTPMediaType) async throws -> Response {
        let path = "/sprites/\(filename)"
        guard FileManager.default.fileExists(atPath: path) else {
            return Response(status: .notFound)
        }
        return try await req.fileio.asyncStreamFile(at: path, mediaType: mediaType)
    }
    app.get("sprites", "sprite.json") { req in
        try await serveSpriteFile(req, filename: "sprite.json", mediaType: .json)
    }
    app.get("sprites", "sprite.png") { req in
        try await serveSpriteFile(req, filename: "sprite.png", mediaType: .png)
    }
    app.get("sprites", "sprite@2x.json") { req in
        try await serveSpriteFile(req, filename: "sprite@2x.json", mediaType: .json)
    }
    app.get("sprites", "sprite@2x.png") { req in
        try await serveSpriteFile(req, filename: "sprite@2x.png", mediaType: .png)
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
            let result = hub.remove(connectionId: connectionId)
            result.events.forEach { send($0.1, to: $0.0) }
            if let clientId = result.clientId {
                Task {
                    try? await hub.stopTracking(clientId: clientId)
                }
            }
        }

        socket.onText { socket, text in
            let incoming: LiveLocationMessage
            do {
                let decoder = JSONDecoder()
                decoder.dateDecodingStrategy = .iso8601
                incoming = try decoder.decode(LiveLocationMessage.self, from: Data(text.utf8))
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

                // --- NEW: Automatically start tracking session on registration ---
                Task {
                    do {
                        let sessionId = try await hub.startTracking(clientId: clientId, sessionTag: incoming.sessionTag)
                        print("🚀 Automatic tracking started for \(clientId). Session: \(sessionId)")
                    } catch {
                        print("❌ Failed to start automatic tracking: \(error)")
                    }
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

                let payload = LocationPayload(latitude: latitude, longitude: longitude, timestamp: incoming.timestamp, isManual: incoming.isManual)
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

            case "admin.start_tracking":
                guard let clientId = incoming.clientId else {
                    send(ServerEvent(type: "error", message: "clientId is required to start tracking."), to: socket)
                    return
                }
                Task {
                    do {
                        let sessionId = try await hub.startTracking(clientId: clientId, sessionTag: incoming.sessionTag)
                        send(ServerEvent(type: "admin.tracking_started", clientId: clientId, message: "Session ID: \(sessionId)"), to: socket)
                    } catch {
                        send(ServerEvent(type: "error", message: "Failed to start tracking: \(error)"), to: socket)
                    }
                }

            case "admin.stop_tracking":
                guard let clientId = incoming.clientId else {
                    send(ServerEvent(type: "error", message: "clientId is required to stop tracking."), to: socket)
                    return
                }
                Task {
                    do {
                        if let sessionId = try await hub.stopTracking(clientId: clientId) {
                            send(ServerEvent(type: "admin.tracking_stopped", clientId: clientId, message: "Stopped Session: \(sessionId)"), to: socket)
                        } else {
                            send(ServerEvent(type: "error", message: "No active session for this client."), to: socket)
                        }
                    } catch {
                        send(ServerEvent(type: "error", message: "Failed to stop tracking: \(error)"), to: socket)
                    }
                }

            default:
                send(ServerEvent(type: "error", message: "Unknown message type: \(incoming.type)."), to: socket)
            }
        }
    }
}
