@testable import GeoSyncBackend
import VaporTesting
import Testing

@Suite("App Tests")
struct GeoSyncBackendTests {
    @Test("Health endpoint reports that the relay is running")
    func health() async throws {
        try await withApp(configure: configure) { app in
            try await app.testing().test(.GET, "health", afterResponse: { res async in
                #expect(res.status == .ok)
                #expect(res.body.string.contains(#""status":"ok""#))
            })
        }
    }
}
