package project

import com.amplitude.project.DeploymentsResponse
import com.amplitude.project.ProjectApiV1
import com.amplitude.util.json
import com.sun.net.httpserver.HttpServer
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.utils.io.ByteReadChannel
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import org.junit.Test
import test.deployment
import test.toSerialDeployment
import java.net.InetSocketAddress
import kotlin.test.assertEquals

class ProjectApiTest {
    private val managementKey = "managementKey"
    private val serverUrl = "https://experiment.amplitude.com/"

    @Test
    fun `get deployments, success`(): Unit =
        runBlocking {
            val deploymentA = deployment("a")
            val deploymentB = deployment("b")
            val expected = listOf(deploymentA, deploymentB)
            val serialDeployments = expected.map { it.toSerialDeployment() }
            val mockEngine =
                MockEngine { request ->
                    respond(
                        content = ByteReadChannel(json.encodeToString(DeploymentsResponse(serialDeployments))),
                        status = HttpStatusCode.OK,
                    )
                }
            val api = ProjectApiV1(serverUrl, managementKey, mockEngine)
            val actual = api.getDeployments()
            assertEquals(expected, actual)
            val request = mockEngine.requestHistory[0]
            assertEquals(HttpMethod.Get, request.method)
            assertEquals("/api/1/deployments", request.url.encodedPath)
            assertEquals("Bearer $managementKey", request.headers["Authorization"])
        }

    @Test
    fun `get deployments, one deleted, returns only active deployment`(): Unit =
        runBlocking {
            val deploymentA = deployment("a")
            val deploymentB = deployment("b")
            val expected = listOf(deploymentB)
            val serialDeployments =
                listOf(
                    deploymentA.toSerialDeployment(true),
                    deploymentB.toSerialDeployment(),
                )
            val mockEngine =
                MockEngine { request ->
                    respond(
                        content = ByteReadChannel(json.encodeToString(DeploymentsResponse(serialDeployments))),
                        status = HttpStatusCode.OK,
                    )
                }
            val api = ProjectApiV1(serverUrl, managementKey, mockEngine)
            val actual = api.getDeployments()
            assertEquals(expected, actual)
            val request = mockEngine.requestHistory[0]
            assertEquals(HttpMethod.Get, request.method)
            assertEquals("/api/1/deployments", request.url.encodedPath)
            assertEquals("Bearer $managementKey", request.headers["Authorization"])
        }

    @Test
    fun `get deployments over okhttp engine with coroutines 1_11`(): Unit =
        runBlocking {
            val expected = listOf(deployment("okhttp"))
            val responseBody =
                json.encodeToString(
                    DeploymentsResponse(expected.map { it.toSerialDeployment() }),
                ).toByteArray()
            val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            server.createContext("/api/1/deployments") { exchange ->
                exchange.responseHeaders.add("Content-Type", "application/json")
                exchange.sendResponseHeaders(200, responseBody.size.toLong())
                exchange.responseBody.use { it.write(responseBody) }
            }
            server.start()

            try {
                val api = ProjectApiV1("http://127.0.0.1:${server.address.port}/", managementKey)
                assertEquals(expected, api.getDeployments())
            } finally {
                server.stop(0)
            }
        }
}
