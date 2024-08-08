import com.amplitude.getApiAndSecretKey
import io.ktor.http.Headers
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
}
