import Vapor
import Fluent

/// Represents a persistent client device.
final class Client: Model, @unchecked Sendable {
    static let schema = "clients"
    
    @ID(custom: .id)
    var id: String? // This will be the persistent unique ID provided by the client.

    @Field(key: "name")
    var name: String?

    @Timestamp(key: "created_at", on: .create)
    var createdAt: Date?

    @Children(for: \.$client)
    var sessions: [TrackingSession]

    init() { }

    init(id: String, name: String? = nil) {
        self.id = id
        self.name = name
    }
}

/// Represents a specific tracking session (a "trip").
final class TrackingSession: Model, Content, @unchecked Sendable {
    static let schema = "tracking_sessions"
    
    @ID(key: .id)
    var id: UUID?

    @Parent(key: "client_id")
    var client: Client

    @Field(key: "session_tag")
    var sessionTag: String? // UUID or custom tag sent by the client.

    @Field(key: "total_distance_km")
    var totalDistanceKm: Double

    @Timestamp(key: "start_time", on: .create)
    var startTime: Date?

    @Field(key: "end_time")
    var endTime: Date?

    @Children(for: \.$session)
    var points: [LocationPoint]

    init() { }

    init(id: UUID? = nil, clientId: String, sessionTag: String? = nil) {
        self.id = id
        self.$client.id = clientId
        self.sessionTag = sessionTag
        self.totalDistanceKm = 0.0
    }
}

/// Represents an individual breadcrumb in a tracking session.
final class LocationPoint: Model, Content, @unchecked Sendable {
    static let schema = "location_points"
    
    @ID(key: .id)
    var id: UUID?

    @Parent(key: "session_id")
    var session: TrackingSession

    @Field(key: "latitude")
    var latitude: Double

    @Field(key: "longitude")
    var longitude: Double

    @Field(key: "timestamp")
    var timestamp: Date? // Device-provided timestamp

    @Timestamp(key: "received_at", on: .create)
    var receivedAt: Date?

    init() { }

    init(id: UUID? = nil, sessionId: UUID, latitude: Double, longitude: Double, timestamp: Date? = nil) {
        self.id = id
        self.$session.id = sessionId
        self.latitude = latitude
        self.longitude = longitude
        self.timestamp = timestamp
    }
}
