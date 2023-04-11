package com.amplitude

import com.amplitude.assignment.AssignmentConfiguration
import com.amplitude.experiment.evaluation.SkylabUser
import com.amplitude.experiment.evaluation.serialization.SerialExperimentUser
import com.amplitude.plugins.configureLogging
import com.amplitude.plugins.configureMetrics
import com.amplitude.util.RedisConfiguration
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.stringEnv
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
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
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.decodeFromString
import project.ProjectConfiguration
import java.util.Base64

fun main() {
    embeddedServer(Netty, port = 3546, host = "0.0.0.0") {

        val apiKey = checkNotNull(stringEnv("AMPLITUDE_API_KEY"))
        val secretKey = checkNotNull(stringEnv("AMPLITUDE_SECRET_KEY"))
        val deploymentKeys = mutableSetOf<String>()
        stringEnv("AMPLITUDE_DEPLOYMENT_KEY")?.let { deploymentKey ->
            deploymentKeys.add(deploymentKey)
        }

        val assignmentConfiguration = AssignmentConfiguration.fromEnv()
        val projectConfiguration = ProjectConfiguration.fromEnv()
        val redisConfiguration = RedisConfiguration.fromEnv()

        val evaluationProxy = EvaluationProxy(
            apiKey = apiKey,
            secretKey = secretKey,
            deploymentKeys = deploymentKeys,
            projectConfiguration = projectConfiguration,
            assignmentConfiguration = assignmentConfiguration,
            redisConfiguration = redisConfiguration,
        )

        runBlocking {
            evaluationProxy.start()
        }

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
                        evaluationProxy.shutdown()
                        plugin.doShutdown(call)
                    }
                }
            }
        )
        routing {
            get("/api/v1/deployments") {
                call.respond(evaluationProxy.getSerializedDeployments())
            }

            put("/api/v1/deployments/{deployment}") {
                val deployment = this.call.parameters["deployment"]
                try {
                    evaluationProxy.addDeployment(deployment)
                } catch (e: HttpErrorResponseException) {
                    call.respond(HttpStatusCode.fromValue(e.status), e.message)
                    return@put
                }
                call.respond(HttpStatusCode.OK)
            }

            delete("/api/v1/deployments/{deployment}") {
                val deployment = this.call.parameters["deployment"]
                try {
                    evaluationProxy.removeDeployment(deployment)
                } catch (e: HttpErrorResponseException) {
                    call.respond(HttpStatusCode.fromValue(e.status), e.message)
                    return@delete
                }
                call.respond(HttpStatusCode.OK)
            }

            get("/sdk/v1/deployments/{deployment}/flags") {
                val deployment = this.call.parameters["deployment"]
                val result = try {
                    evaluationProxy.getSerializedFlagConfigs(deployment)
                } catch (e: HttpErrorResponseException) {
                    call.respond(HttpStatusCode.fromValue(e.status), e.message)
                    return@get
                }
                call.respond(result)
            }

            get("/sdk/v1/deployments/{deployment}/users/{userId}/cohorts") {
                val deployment = this.call.parameters["deployment"]
                val userId = this.call.parameters["userId"]
                val result = try {
                    evaluationProxy.getSerializedCohortMembershipsForUser(deployment, userId)
                } catch (e: HttpErrorResponseException) {
                    call.respond(HttpStatusCode.fromValue(e.status), e.message)
                    return@get
                }
                call.respond(result)
            }

            get("/sdk/vardata") {
                // Deployment key is included in Authorization header with prefix "Api-Key "
                val deploymentKey = call.request.getDeploymentKey()
                val user = call.request.getUserFromHeader()
                val flagKeys = call.request.getFlagKeys()
                val result = evaluationProxy.serializedEvaluate(deploymentKey, user, flagKeys)
                call.respond(result)
            }

            post("/sdk/vardata") {
                // Deployment key is included in Authorization header with prefix "Api-Key "
                val deploymentKey = call.request.getDeploymentKey()
                val user = call.request.getUserFromBody()
                val flagKeys = call.request.getFlagKeys()
                val result = evaluationProxy.serializedEvaluate(deploymentKey, user, flagKeys)
                call.respond(result)
            }

            get("/v1/vardata") {
                // Deployment key is included in Authorization header with prefix "Api-Key "
                val deploymentKey = call.request.getDeploymentKey()
                val user = call.request.getUserFromQuery()
                val flagKeys = call.request.getFlagKeys()
                val result = evaluationProxy.serializedEvaluate(deploymentKey, user, flagKeys)
                call.respond(result)
            }

            post("/v1/vardata") {
                // Deployment key is included in Authorization header with prefix "Api-Key "
                val deploymentKey = call.request.getDeploymentKey()
                val user = call.request.getUserFromBody()
                val flagKeys = call.request.getFlagKeys()
                val result = evaluationProxy.serializedEvaluate(deploymentKey, user, flagKeys)
                call.respond(result)
            }
        }
    }.start(wait = true)
}

/**
 * Get the deployment key from the call, included in Authorization header with prefix "Api-Key "
 */
private fun ApplicationRequest.getDeploymentKey(): String? {
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
private fun ApplicationRequest.getFlagKeys(): Set<String> {
    val flagKeys: MutableSet<String> = mutableSetOf()
    val headerFlagKeys = call.request.headers["X-Amp-Exp-Flag-Keys"]
    val queryParameterFlagKeys = call.request.queryParameters["flag_key"]
    if (headerFlagKeys != null) {
        val flagKeysJson = Base64.getDecoder().decode(headerFlagKeys).toString(Charsets.UTF_8)
        flagKeys += json.decodeFromString<Set<String>>(flagKeysJson)
    } else if (queryParameterFlagKeys != null) {
        flagKeys += queryParameterFlagKeys.split(",")
    }
    return flagKeys
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
