package com.amplitude

import com.amplitude.cohort.CohortApiV3
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.deployment.DeploymentApiV0
import com.amplitude.deployment.DeploymentConfiguration
import com.amplitude.deployment.DeploymentManager
import com.amplitude.deployment.InMemoryDeploymentStorage
import com.amplitude.plugins.configureLogging
import com.amplitude.plugins.configureMetrics
import com.amplitude.util.HttpErrorResponseException
import com.amplitude.util.getCohortIds
import com.amplitude.util.stringEnv
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

    val apiKey = checkNotNull(stringEnv("AMPLITUDE_API_KEY"))
    val secretKey = checkNotNull(stringEnv("AMPLITUDE_SECRET_KEY"))

    val deploymentConfiguration = DeploymentConfiguration()
    val deploymentApi = DeploymentApiV0()
    val deploymentStorage = InMemoryDeploymentStorage()
    val cohortApi = CohortApiV3(apiKey = apiKey, secretKey = secretKey)
    val cohortStorage = InMemoryCohortStorage()

    val deploymentManager = DeploymentManager(
        deploymentConfiguration,
        deploymentApi,
        deploymentStorage,
        cohortApi,
        cohortStorage
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
                // Validate deployment key with amplitude
                try {
                    deploymentApi.getFlagConfigs(deployment)
                } catch (e: HttpErrorResponseException) {
                    when (e.statusCode.value) {
                        in 400..499 -> call.respond(e.statusCode)
                        else -> call.respond(HttpStatusCode.ServiceUnavailable)
                    }
                    return@put
                }
                deploymentStorage.putDeployment(deployment)
                call.respond(HttpStatusCode.OK)
            }
            delete("/api/v1/deployments/{deployment}") {
                val deployment = this.call.parameters["deployment"]
                if (deployment.isNullOrEmpty() || !deployment.startsWith("server-")) {
                    call.respond(status = HttpStatusCode.BadRequest, "Invalid deployment")
                    return@delete
                }
                deploymentStorage.removeDeployment(deployment)
                call.respond(HttpStatusCode.OK)
            }
            get("/sdk/v1/deployments/{deployment}/flags") {
                val deployment = this.call.parameters["deployment"]
                if (deployment.isNullOrEmpty() || !deployment.startsWith("server-")) {
                    call.respond(status = HttpStatusCode.BadRequest, "Invalid deployment")
                    return@get
                }
                val flagConfigs = deploymentStorage.getFlagConfigs(deployment)
                if (flagConfigs == null) {
                    call.respond(status = HttpStatusCode.NotFound, "Unknown deployment")
                    return@get
                }
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
                val cohortIds = deploymentStorage.getCohortIds(deployment)
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
