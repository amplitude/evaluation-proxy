import com.amplitude.getApiAndSecretKey
import com.amplitude.getUserFromBody
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.Headers
import io.ktor.server.application.call
import io.ktor.server.response.respondText
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import io.ktor.util.encodeBase64
import kotlin.test.Test
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
}
