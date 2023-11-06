package com.amplitude.project

import com.amplitude.DeploymentsFetch
import com.amplitude.DeploymentsFetchFailure
import com.amplitude.Metrics
import com.amplitude.deployment.Deployment
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
private data class DeploymentsResponse(
    val deployments: List<SerialDeployment>
)

@Serializable
internal data class SerialDeployment(
    val id: String,
    val projectId: String,
    val label: String,
    val key: String,
    val deleted: Boolean
)

private fun SerialDeployment.toDeployment(): Deployment? {
    if (deleted) return null
    return Deployment(id, projectId, label, key)
}

internal interface ProjectApi {
    suspend fun getDeployments(): List<Deployment>
}

internal class ProjectApiV1(private val managementKey: String) : ProjectApi {

    companion object {
        val log by logger()
    }

    private val client = HttpClient(OkHttp) {
        install(HttpTimeout) {
            socketTimeoutMillis = 30000
        }
    }

    override suspend fun getDeployments(): List<Deployment> =
        Metrics.with({ DeploymentsFetch }, { e -> DeploymentsFetchFailure(e) }) {
            log.trace("getDeployments: start")
            val response = retry(onFailure = { e -> log.error("Get deployments failed: $e") }) {
                client.get(MANAGEMENT_SERVER_URL, "/api/1/deployments") {
                    headers {
                        set("Authorization", "Bearer $managementKey")
                        set("Accept", "application/json")
                    }
                }
            }
            json.decodeFromString<DeploymentsResponse>(response.body())
                .deployments
                .mapNotNull { it.toDeployment() }
                .also { log.trace("getDeployments: end") }
        }
}
