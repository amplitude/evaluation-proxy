package com.amplitude.project

import com.amplitude.util.get
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.retry
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.headers
import kotlinx.serialization.Serializable

private const val MANAGEMENT_SERVER_URL = "https://experiment.amplitude.com"

@Serializable
internal data class Deployment(
    val id: String,
    val projectId: String,
    val label: String,
    val key: String,
    val deleted: Boolean,
)

internal interface ProjectApi {
    suspend fun getDeployments(): List<Deployment>
}

internal class ProjectApiV1(private val managementKey: String): ProjectApi {

    companion object {
        val log by logger()
    }

    private val client = HttpClient(OkHttp) {
        install(HttpTimeout) {
            socketTimeoutMillis = 30000
        }
    }
    override suspend fun getDeployments(): List<Deployment> {
        log.debug("getDeployments: start")
        val response = retry(onFailure = { e -> log.error("Get deployments failed: $e") }) {
            client.get(MANAGEMENT_SERVER_URL, "/api/1/deployments") {
                headers {
                    set("Authorization", "Bearer $managementKey")
                    set("Accept", "application/json")
                }
            }
        }
        return json.decodeFromString<List<Deployment>>(response.body())
            .filter { !it.deleted }
            .also { log.debug("getDeployments: end") }
    }
}
