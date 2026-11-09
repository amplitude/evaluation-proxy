import com.amplitude.getApiAndSecretKey
import com.amplitude.getUserFromBody
import com.amplitude.respondGzippedJson
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsBytes
import io.ktor.client.statement.bodyAsText
import io.ktor.http.Headers
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import io.ktor.util.encodeBase64
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class ServerTest {
    @Test
    fun `test get api and secret key`() {
        val apiKey = "api"
        val secretKey = "secret"
        val headers =
            Headers.build {
                set("Authorization", "Basic ${"$apiKey:$secretKey".encodeBase64()}")
            }

        val result = headers.getApiAndSecretKey()
        assertEquals(apiKey, result.first)
        assertEquals(secretKey, result.second)
    }

    @Test
    fun `test get user from body`() =
        testApplication {
            application {
                routing {
                    post("/") {
                        val user = call.request.getUserFromBody()
                        call.respondText(user["user_id"].toString())
                    }
                }
            }

            val response =
                client.post("/") {
                    setBody("""{"user_id":"test-user"}""")
                }
            assertEquals("test-user", response.bodyAsText())
        }

    @Test
    fun `test respond gzipped json streams the full payload with gzip headers`() =
        testApplication {
            // Not a multiple of the write chunk size, so the final partial chunk is exercised
            val payload = Random(42).nextBytes(1_000_003)
            application {
                routing {
                    get("/blob") { call.respondGzippedJson(payload) }
                }
            }

            val response = client.get("/blob")
            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("gzip", response.headers[HttpHeaders.ContentEncoding])
            assertEquals("application/json", response.headers[HttpHeaders.ContentType])
            assertEquals(payload.size.toString(), response.headers[HttpHeaders.ContentLength])
            assertContentEquals(payload, response.bodyAsBytes())
        }

    @Test
    fun `test respond gzipped json serves more concurrent requests than write permits`() =
        testApplication {
            val payload = Random(7).nextBytes(256 * 1024)
            application {
                routing {
                    get("/blob") { call.respondGzippedJson(payload) }
                }
            }

            // More requests than the concurrent write cap: all must complete, so permits are released
            val bodies =
                coroutineScope {
                    (1..300).map { async { client.get("/blob").bodyAsBytes() } }.awaitAll()
                }
            assertEquals(300, bodies.size)
            bodies.forEach { assertContentEquals(payload, it) }
        }
}
