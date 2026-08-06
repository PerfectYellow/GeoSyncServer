import Vapor
import Fluent
import FluentSQLiteDriver

/// configures your application
func configure(_ app: Application) async throws {
    // Main App Database
    app.databases.use(.sqlite(.file("db.sqlite")), as: .sqlite)

    // Register Migrations
    app.migrations.add(CreateClient())
    app.migrations.add(CreateTrackingSession())
    app.migrations.add(CreateLocationPoint())

    // Run migrations automatically
    try await app.autoMigrate()

    // --- Configure JSON Date Encoding Strategy ---
    let encoder = JSONEncoder()
    encoder.dateEncodingStrategy = .iso8601
    let decoder = JSONDecoder()
    decoder.dateDecodingStrategy = .iso8601
    ContentConfiguration.global.use(encoder: encoder, for: .json)
    ContentConfiguration.global.use(decoder: decoder, for: .json)

    // register routes
    try routes(app)
}
