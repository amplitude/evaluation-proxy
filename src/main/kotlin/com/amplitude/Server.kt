package com.amplitude

import com.amplitude.assignment.AmplitudeAssignmentTracker
import com.amplitude.assignment.Assignment
import com.amplitude.assignment.AssignmentConfiguration
import com.amplitude.cohort.CohortApiV3
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.deployment.DeploymentApiV1
import com.amplitude.deployment.DeploymentConfiguration
import com.amplitude.deployment.DeploymentManager
import com.amplitude.deployment.InMemoryDeploymentStorage
import com.amplitude.experiment.evaluation.EvaluationEngineImpl
import com.amplitude.experiment.evaluation.FlagResult
import com.amplitude.experiment.evaluation.SkylabUser
import com.amplitude.experiment.evaluation.serialization.SerialExperimentUser
import com.amplitude.experiment.evaluation.serialization.SerialFlagConfig
import com.amplitude.experiment.evaluation.serialization.SerialVariant
import com.amplitude.plugins.configureLogging
import com.amplitude.plugins.configureMetrics
import com.amplitude.util.HttpErrorResponseException
import com.amplitude.util.json
import com.amplitude.util.stringEnv
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.install
import io.ktor.server.engine.ShutDownUrl
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.ApplicationRequest
import io.ktor.server.request.uri
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.routing
import io.ktor.util.toByteArray
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.decodeFromString
import java.util.Base64

const val VERSION = "0.1.0"

val apiKey = checkNotNull(stringEnv("AMPLITUDE_API_KEY"))
val secretKey = checkNotNull(stringEnv("AMPLITUDE_SECRET_KEY"))
val baseDeploymentKey = stringEnv("AMPLITUDE_DEPLOYMENT_KEY")

val engine = EvaluationEngineImpl()

val assignmentConfiguration = AssignmentConfiguration.fromEnv()
val assignmentTracker = AmplitudeAssignmentTracker(apiKey, assignmentConfiguration)

val deploymentConfiguration = DeploymentConfiguration.fromEnv()
val deploymentApi = DeploymentApiV1()
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

fun main() {

    runBlocking {
        if (baseDeploymentKey != null) {
            deploymentStorage.putDeployment(baseDeploymentKey)
        }
        deploymentManager.start()
    }

    embeddedServer(Netty, port = 3546, host = "0.0.0.0") {
        configureLogging()
        configureMetrics()
        install(ContentNegotiation) {
            json()
        }
        // Custom shutdown plugin
        install(
            createApplicationPlugin("shutdown") {
                val plugin = ShutDownUrl("/shutdown") { 0 }
                onCall { call ->
                    if (call.request.uri == plugin.url) {
                        deploymentManager.stop()
                        plugin.doShutdown(call)
                    }
                }
            }
        )
        routing {

            get("/api/v1/deployments") {
                call.respond(deploymentStorage.getDeployments())
            }

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
                call.respond(flagConfigs.map { SerialFlagConfig(it) })
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

            get("/sdk/vardata") {
                call.evaluate(ApplicationRequest::getUserFromHeader)
            }
            post("/sdk/vardata") {
                call.evaluate(ApplicationRequest::getUserFromBody)
            }
            get("/v1/vardata") {
                call.evaluate(ApplicationRequest::getUserFromQuery)
            }
            post("/v1/vardata") {
                call.evaluate(ApplicationRequest::getUserFromBody)
            }
        }
    }.start(wait = true)
}

suspend fun ApplicationCall.evaluate(
    userProvider: suspend ApplicationRequest.() -> SkylabUser
) {
    // Deployment key is included in Authorization header with prefix "Api-Key "
    val deploymentKey = this.request.getApiKey()
    if (deploymentKey == null) {
        this.respond(HttpStatusCode.Unauthorized, "Invalid deployment key.")
        return
    }
    // Get flag configs for the deployment from storage.
    val flagConfigs = deploymentStorage.getFlagConfigs(deploymentKey)
    if (flagConfigs == null || flagConfigs.isEmpty()) {
        this.respond<Map<String, SerialVariant>>(mapOf())
        return
    }
    // Get flag keys and user from request.
    val flagKeys = this.request.getFlagKeys()
    val user = this.request.userProvider()
    // Enrich user with cohort IDs.
    val enrichedUser = user.userId?.let { userId ->
        user.copy(cohortIds = cohortStorage.getCohortMembershipsForUser(userId))
    }

    // Evaluate results
    val result = engine.evaluate(flagConfigs, enrichedUser)
    if (enrichedUser != null) {
        coroutineScope {
            launch {
                assignmentTracker.track(Assignment(enrichedUser, result))
            }
        }
    }
    val response = result.filterDeployedVariants(flagKeys)
    this.respond(response)
}

/**
 * Get the deployment key from the call, included in Authorization header with prefix "Api-Key "
 */
private fun ApplicationRequest.getApiKey(): String? {
    val deploymentKey = this.headers["Authorization"]
    if (deploymentKey == null || !deploymentKey.startsWith("Api-Key", ignoreCase = true)) {
        return null
    }
    return deploymentKey.substring("Api-Key ".length)
}

/**
 * Get the flag keys from the request. Either contained in header or query params.
 * Flag keys are used to filter the results to only required flags.
 */
private fun ApplicationRequest.getFlagKeys(): List<String> {
    val flagKeys: MutableList<String> = mutableListOf()
    val headerFlagKeys = call.request.headers["X-Amp-Exp-Flag-Keys"]
    val queryParameterFlagKeys = call.request.queryParameters["flag_key"]
    if (headerFlagKeys != null) {
        val flagKeysJson = Base64.getDecoder().decode(headerFlagKeys).toString(Charsets.UTF_8)
        flagKeys += json.decodeFromString<List<String>>(flagKeysJson)
    } else if (queryParameterFlagKeys != null) {
        flagKeys += queryParameterFlagKeys.split(",")
    }
    return flagKeys
}

/**
 * Filter only non-default, deployed variants from the results that are included if flag keys (if not empty).
 */
private fun Map<String, FlagResult>.filterDeployedVariants(flagKeys: List<String>): Map<String, SerialVariant> {
    return filter { entry ->
        val isVariant = !entry.value.isDefaultVariant
        val isIncluded = (flagKeys.isEmpty() || flagKeys.contains(entry.key))
        val isDeployed = entry.value.deployed
        isVariant && isIncluded && isDeployed
    }.mapValues { entry ->
        SerialVariant(entry.value.variant)
    }.toMap()
}

/**
 * Get the user from the header. Used for SDK GET requests.
 */
private fun ApplicationRequest.getUserFromHeader(): SkylabUser {
    val b64User = this.headers["X-Amp-Exp-User"]
    val userJson = Base64.getDecoder().decode(b64User).toString(Charsets.UTF_8)
    return json.decodeFromString<SerialExperimentUser>(userJson).convert()
}

/**
 * Get the user from the body. Used for SDK/REST POST requests.
 */
private suspend fun ApplicationRequest.getUserFromBody(): SkylabUser {
    val userJson = this.receiveChannel().toByteArray().toString(Charsets.UTF_8)
    return json.decodeFromString<SerialExperimentUser>(userJson).convert()
}

/**
 * Get the user from the query. Used for REST GET requests.
 */
private fun ApplicationRequest.getUserFromQuery(): SkylabUser {
    val userId = this.queryParameters["user_id"]
    val deviceId = this.queryParameters["device_id"]
    val context = this.queryParameters["context"]
    var user = if (context != null) {
        json.decodeFromString<SerialExperimentUser>(context).convert()
    } else {
        SkylabUser()
    }
    if (userId != null) {
        user = user.copy(userId = userId)
    }
    if (deviceId != null) {
        user = user.copy(deviceId = deviceId)
    }
    return user
}
