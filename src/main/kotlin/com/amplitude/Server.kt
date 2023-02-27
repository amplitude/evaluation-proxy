package com.amplitude

import com.amplitude.cohort.CohortApiV3
import com.amplitude.cohort.CohortLoader
import com.amplitude.cohort.CohortLoaderConfiguration
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.deployment.DeploymentApiV0
import com.amplitude.deployment.DeploymentManager
import com.amplitude.deployment.DeploymentManagerConfiguration
import com.amplitude.deployment.InMemoryDeploymentStorage
import com.amplitude.plugins.configureLogging
import com.amplitude.plugins.configureMetrics
import com.amplitude.util.getCohortIds
import com.amplitude.util.toSerial
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.ShutDownUrl
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.put
import io.ktor.server.routing.routing
import kotlinx.coroutines.runBlocking

const val DEFAULT_HOST = "0.0.0.0"
const val DEFAULT_PORT = 3546

fun main() {
    val cohortConfiguration = CohortLoaderConfiguration()
    val cohortApi = CohortApiV3(apiKey = "", secretKey = "")
    val cohortStorage = InMemoryCohortStorage()
    val cohortLoader = CohortLoader(
        cohortConfiguration,
        cohortApi,
        cohortStorage,
    )

    val deploymentConfiguration = DeploymentManagerConfiguration()
    val deploymentApi = DeploymentApiV0()
    val deploymentStorage = InMemoryDeploymentStorage()
    val deploymentManager = DeploymentManager(
        deploymentConfiguration,
        deploymentApi,
        deploymentStorage,
        cohortLoader,
    )

    runBlocking {
        deploymentManager.start()
    }

    embeddedServer(Netty, port = DEFAULT_PORT, host = DEFAULT_HOST) {
        configureLogging()
        configureMetrics()
        install(ContentNegotiation) {
            json()
        }
        install(ShutDownUrl.ApplicationCallPlugin) {
            shutDownUrl = "/shutdown"
            exitCodeSupplier = { 0 }
        }
        routing {
            put("/api/v1/deployments/{deployment}") {
                val deployment = this.call.parameters["deployment"]
                if (deployment.isNullOrEmpty() || !deployment.startsWith("server-")) {
                    call.respond(status = HttpStatusCode.BadRequest, "Invalid deployment")
                    return@put
                }
                deploymentStorage.putDeployment(deployment)
            }
            delete("/api/v1/deployments/{deployment}") {
                val deployment = this.call.parameters["deployment"]
                if (deployment.isNullOrEmpty() || !deployment.startsWith("server-")) {
                    call.respond(status = HttpStatusCode.BadRequest, "Invalid deployment")
                    return@delete
                }
                deploymentStorage.removeDeployment(deployment)
            }
            get("/sdk/v1/deployments/{deployment}/flags") {
                val deployment = this.call.parameters["deployment"]
                if (deployment.isNullOrEmpty() || !deployment.startsWith("server-")) {
                    call.respond(status = HttpStatusCode.BadRequest, "Invalid deployment")
                    return@get
                }
                val flagConfigs = deploymentStorage.getFlagConfigs(deployment) ?: listOf()
                call.respond(flagConfigs.map { it.toSerial() })
            }
            get("/sdk/v1/deployments/{deployment}/users/{userId}/cohorts") {
                val deployment = this.call.parameters["deployment"]
                if (deployment.isNullOrEmpty() || !deployment.startsWith("server-")) {
                    call.respond(status = HttpStatusCode.BadRequest, "Invalid deployment")
                    return@get
                }
                val userId = this.call.parameters["userId"]
                if (userId.isNullOrEmpty()) {
                    call.respond(status = HttpStatusCode.BadRequest, "Invalid user ID")
                    return@get
                }
                val cohortIds = deploymentStorage.getFlagConfigs(deployment)?.getCohortIds()
                if (cohortIds == null) {
                    call.respond(status = HttpStatusCode.NotFound, "Unknown deployment")
                    return@get
                }
                val memberships = cohortStorage.getCohortMembershipsForUser(userId, cohortIds)
                call.respond(memberships)
            }
        }
    }.start(wait = true)
}
