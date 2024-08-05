package cohort

import com.amplitude.cohort.Cohort
import com.amplitude.cohort.CohortApiV1
import com.amplitude.cohort.CohortNotModifiedException
import com.amplitude.cohort.CohortTooLargeException
import com.amplitude.cohort.GetCohortResponse
import com.amplitude.util.HttpErrorException
import com.amplitude.util.RetryConfig
import com.amplitude.util.json
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.Parameters
import io.ktor.utils.io.ByteReadChannel
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import java.io.IOException
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class CohortApiTest {
    private val apiKey = "api"
    private val secretKey = "secret"
    private val serverUrl = "https://api.lab.amplitude.com/"
    private val token = Base64.getEncoder().encodeToString("$apiKey:$secretKey".toByteArray(Charsets.UTF_8))
    private val fastRetryConfig =
        RetryConfig(
            times = 5,
            initialDelayMillis = 1,
            maxDelay = 1,
            factor = 1.0,
        )

    @Test
    fun `without existing cohort, success`(): Unit =
        runBlocking {
            val expected = Cohort("a", "User", 1, 100L, setOf("1"))
            val mockEngine =
                MockEngine { request ->
                    respond(
                        content = ByteReadChannel(json.encodeToString(GetCohortResponse.fromCohort(expected))),
                        status = HttpStatusCode.OK,
                    )
                }
            val api = CohortApiV1(serverUrl, apiKey, secretKey, mockEngine)
            val actual = api.getCohort("a", null, Int.MAX_VALUE)
            assertEquals(expected, actual)
            val request = mockEngine.requestHistory[0]
            assertEquals(HttpMethod.Get, request.method)
            assertEquals("/sdk/v1/cohort/a", request.url.encodedPath)
            assertEquals(
                Parameters.build {
                    set("maxCohortSize", "${Int.MAX_VALUE}")
                },
                request.url.parameters,
            )
            assertTrue(request.headers["X-Amp-Exp-Library"]!!.startsWith("evaluation-proxy/"))
            assertEquals("Basic $token", request.headers["Authorization"])
        }

    @Test
    fun `with existing cohort, success`(): Unit =
        runBlocking {
            val expected = Cohort("a", "User", 1, 100L, setOf("1"))
            val mockEngine =
                MockEngine { request ->
                    respond(
                        content = ByteReadChannel(json.encodeToString(GetCohortResponse.fromCohort(expected))),
                        status = HttpStatusCode.OK,
                    )
                }
            val api = CohortApiV1(serverUrl, apiKey, secretKey, mockEngine)
            val actual = api.getCohort("a", 99, Int.MAX_VALUE)
            assertEquals(expected, actual)
            val request = mockEngine.requestHistory[0]
            assertEquals(HttpMethod.Get, request.method)
            assertEquals("/sdk/v1/cohort/a", request.url.encodedPath)
            assertEquals(
                Parameters.build {
                    set("maxCohortSize", "${Int.MAX_VALUE}")
                    set("lastModified", "99")
                },
                request.url.parameters,
            )
            assertTrue(request.headers["X-Amp-Exp-Library"]!!.startsWith("evaluation-proxy/"))
            assertEquals("Basic $token", request.headers["Authorization"])
        }

    @Test
    fun `with existing cohort, cohort not modified, no retries, throws`(): Unit =
        runBlocking {
            val mockEngine =
                MockEngine { request ->
                    respond(
                        content = "",
                        status = HttpStatusCode.NoContent,
                    )
                }
            val api = CohortApiV1(serverUrl, apiKey, secretKey, mockEngine)
            try {
                api.getCohort("a", 99, Int.MAX_VALUE)
                fail("Expected getCohort call to fail with CohortNotModifiedException")
            } catch (e: CohortNotModifiedException) {
                // Success
            }
        }

    @Test
    fun `without existing cohort, cohort too large, no retries, throws`(): Unit =
        runBlocking {
            val mockEngine =
                MockEngine { request ->
                    respond(
                        content = "",
                        status = HttpStatusCode.PayloadTooLarge,
                    )
                }
            val api = CohortApiV1(serverUrl, apiKey, secretKey, mockEngine)
            try {
                api.getCohort("a", 99, Int.MAX_VALUE)
                fail("Expected getCohort call to fail with CohortTooLargeException")
            } catch (e: CohortTooLargeException) {
                // Success
            }
        }

    @Test
    fun `request failures, retries, throws`(): Unit =
        runBlocking {
            var failureCounter = 0
            val mockEngine =
                MockEngine { _ ->
                    failureCounter++
                    throw IOException("test")
                }
            val api = CohortApiV1(serverUrl, apiKey, secretKey, mockEngine, fastRetryConfig)
            try {
                api.getCohort("a", 99, Int.MAX_VALUE)
                fail("Expected getCohort call to fail with IOException")
            } catch (e: IOException) {
                // Success
            }
            assertEquals(5, failureCounter)
        }

    @Test
    fun `request server error responses, retries, throws`(): Unit =
        runBlocking {
            val mockEngine =
                MockEngine { _ ->
                    respond(
                        content = "",
                        status = HttpStatusCode.InternalServerError,
                    )
                }
            val api = CohortApiV1(serverUrl, apiKey, secretKey, mockEngine, fastRetryConfig)
            try {
                api.getCohort("a", 99, Int.MAX_VALUE)
                fail("Expected getCohort call to fail with HttpErrorException")
            } catch (e: HttpErrorException) {
                // Success
            }
            assertEquals(5, mockEngine.responseHistory.size)
        }

    @Test
    fun `request client too many requests, retries, throws`(): Unit =
        runBlocking {
            val mockEngine =
                MockEngine { _ ->
                    respond(
                        content = "",
                        status = HttpStatusCode.TooManyRequests,
                    )
                }
            val api = CohortApiV1(serverUrl, apiKey, secretKey, mockEngine, fastRetryConfig)
            try {
                api.getCohort("a", 99, Int.MAX_VALUE)
                fail("Expected getCohort call to fail with HttpErrorException")
            } catch (e: HttpErrorException) {
                assertEquals(HttpStatusCode.TooManyRequests, mockEngine.responseHistory[0].statusCode)
            }
            assertEquals(5, mockEngine.responseHistory.size)
        }

    @Test
    fun `request client error, no retries, throws`(): Unit =
        runBlocking {
            val mockEngine =
                MockEngine { _ ->
                    respond(
                        content = "",
                        status = HttpStatusCode.NotFound,
                    )
                }
            val api = CohortApiV1(serverUrl, apiKey, secretKey, mockEngine, fastRetryConfig)
            try {
                api.getCohort("a", 99, Int.MAX_VALUE)
                fail("Expected getCohort call to fail with HttpErrorException")
            } catch (e: HttpErrorException) {
                assertEquals(HttpStatusCode.NotFound, mockEngine.responseHistory[0].statusCode)
            }
            assertEquals(1, mockEngine.responseHistory.size)
        }

    @Test
    fun `request errors, eventual success`(): Unit =
        runBlocking {
            var i = 0
            val expected = Cohort("a", "User", 1, 100L, setOf("1"))
            val mockEngine =
                MockEngine { _ ->
                    i++
                    when (i) {
                        1 -> respond(content = "", status = HttpStatusCode.InternalServerError)
                        2 -> respond(content = "", status = HttpStatusCode.TooManyRequests)
                        3 -> respond(content = "", status = HttpStatusCode.BadGateway)
                        4 -> respond(content = "", status = HttpStatusCode.GatewayTimeout)
                        5 ->
                            respond(
                                content = ByteReadChannel(json.encodeToString(GetCohortResponse.fromCohort(expected))),
                                status = HttpStatusCode.OK,
                            )
                        else -> fail("unexpected number of requests")
                    }
                }
            val api = CohortApiV1(serverUrl, apiKey, secretKey, mockEngine, fastRetryConfig)
            val actual = api.getCohort("a", 99, Int.MAX_VALUE)
            assertEquals(5, mockEngine.responseHistory.size)
            assertEquals(expected, actual)
        }
}
