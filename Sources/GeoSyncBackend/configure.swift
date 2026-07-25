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

    // register routes
    try routes(app)
}
