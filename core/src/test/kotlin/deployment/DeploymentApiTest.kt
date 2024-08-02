package deployment

import com.amplitude.deployment.DeploymentApiV2
import com.amplitude.util.HttpErrorException
import com.amplitude.util.RetryConfig
import com.amplitude.util.json
import test.flag
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.Parameters
import io.ktor.utils.io.ByteReadChannel
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import org.junit.Test
import java.io.IOException
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class DeploymentApiTest {
    private val deploymentKey = "deployment"
    private val serverUrl = "https://api.lab.amplitude.com/"
    private val fastRetryConfig = RetryConfig(
        times = 5,
        initialDelayMillis = 1,
        maxDelay = 1,
        factor = 1.0
    )

    @Test
    fun `get flags, success`(): Unit = runBlocking {
        val expected = listOf(flag())
        val mockEngine = MockEngine { request ->
            respond(
                content = ByteReadChannel(json.encodeToString(expected)),
                status = HttpStatusCode.OK
            )
        }
        val api = DeploymentApiV2(serverUrl, mockEngine, fastRetryConfig)
        val actual = api.getFlagConfigs(deploymentKey)
        assertEquals(expected, actual)
        val request = mockEngine.requestHistory[0]
        assertEquals(HttpMethod.Get, request.method)
        assertEquals("/sdk/v2/flags", request.url.encodedPath)
        assertEquals(
            Parameters.build {
                set("v", "0")
            },
            request.url.parameters
        )
        assertTrue(request.headers["X-Amp-Exp-Library"]!!.startsWith("evaluation-proxy/"))
        assertEquals("Api-Key $deploymentKey", request.headers["Authorization"])
    }

    @Test
    fun `request failures, retries, throws`(): Unit = runBlocking {
        var failureCounter = 0
        val mockEngine = MockEngine { _ ->
            failureCounter++
            throw IOException("test")
        }
        val api = DeploymentApiV2(serverUrl, mockEngine, fastRetryConfig)
        try {
            api.getFlagConfigs(deploymentKey)
            fail("Expected getFlagConfigs call to fail with IOException")
        } catch (e: IOException) {
            // Success
        }
        assertEquals(5, failureCounter)
    }

    @Test
    fun `request server error responses, retries, throws`(): Unit = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = "",
                status = HttpStatusCode.InternalServerError
            )
        }
        val api = DeploymentApiV2(serverUrl, mockEngine, fastRetryConfig)
        try {
            api.getFlagConfigs(deploymentKey)
            fail("Expected getFlagConfigs call to fail with HttpErrorException")
        } catch (e: HttpErrorException) {
            // Success
        }
        assertEquals(5, mockEngine.responseHistory.size)
    }

    @Test
    fun `request client too many requests, retries, throws`(): Unit = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = "",
                status = HttpStatusCode.TooManyRequests
            )
        }
        val api = DeploymentApiV2(serverUrl, mockEngine, fastRetryConfig)
        try {
            api.getFlagConfigs(deploymentKey)
            fail("Expected getFlagConfigs call to fail with HttpErrorException")
        } catch (e: HttpErrorException) {
            assertEquals(HttpStatusCode.TooManyRequests, mockEngine.responseHistory[0].statusCode)
        }
        assertEquals(5, mockEngine.responseHistory.size)
    }

    @Test
    fun `request client error, no retries, throws`(): Unit = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = "",
                status = HttpStatusCode.NotFound
            )
        }
        val api = DeploymentApiV2(serverUrl, mockEngine, fastRetryConfig)
        try {
            api.getFlagConfigs(deploymentKey)
            fail("Expected getFlagConfigs call to fail with HttpErrorException")
        } catch (e: HttpErrorException) {
            assertEquals(HttpStatusCode.NotFound, mockEngine.responseHistory[0].statusCode)
        }
        assertEquals(1, mockEngine.responseHistory.size)
    }

    @Test
    fun `request errors, eventual success`(): Unit = runBlocking {
        var i = 0
        val expected = listOf(flag())
        val mockEngine = MockEngine { _ ->
            i++
            when (i) {
                1 -> respond(content = "", status = HttpStatusCode.InternalServerError)
                2 -> respond(content = "", status = HttpStatusCode.TooManyRequests)
                3 -> respond(content = "", status = HttpStatusCode.BadGateway)
                4 -> respond(content = "", status = HttpStatusCode.GatewayTimeout)
                5 -> respond(
                    content = ByteReadChannel(json.encodeToString(expected)),
                    status = HttpStatusCode.OK
                )
                else -> fail("unexpected number of requests")
            }
        }
        val api = DeploymentApiV2(serverUrl, mockEngine, fastRetryConfig)
        val actual = api.getFlagConfigs(deploymentKey)
        assertEquals(5, mockEngine.responseHistory.size)
        assertEquals(expected, actual)
    }
}
