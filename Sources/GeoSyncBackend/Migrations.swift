import Fluent

struct CreateClient: AsyncMigration {
    func prepare(on database: any Database) async throws {
        try await database.schema("clients")
            .field("id", .string, .identifier(auto: false))
            .field("name", .string)
            .field("created_at", .datetime)
            .create()
    }

    func revert(on database: any Database) async throws {
        try await database.schema("clients").delete()
    }
}

struct CreateTrackingSession: AsyncMigration {
    func prepare(on database: any Database) async throws {
        try await database.schema("tracking_sessions")
            .id()
            .field("client_id", .string, .required, .references("clients", "id"))
            .field("session_tag", .string)
            .field("total_distance_km", .double, .required)
            .field("start_time", .datetime)
            .field("end_time", .datetime)
            .create()
    }

    func revert(on database: any Database) async throws {
        try await database.schema("tracking_sessions").delete()
    }
}

struct CreateLocationPoint: AsyncMigration {
    func prepare(on database: any Database) async throws {
        try await database.schema("location_points")
            .id()
            .field("session_id", .uuid, .required, .references("tracking_sessions", "id"))
            .field("latitude", .double, .required)
            .field("longitude", .double, .required)
            .field("timestamp", .datetime)
            .field("received_at", .datetime)
            .create()
    }

    func revert(on database: any Database) async throws {
        try await database.schema("location_points").delete()
    }
}
