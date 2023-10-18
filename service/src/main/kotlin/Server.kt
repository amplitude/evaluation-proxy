package com.amplitude

import io.ktor.server.application.Application
import com.amplitude.plugins.configureLogging
import com.amplitude.plugins.configureMetrics
import com.amplitude.util.json
import com.amplitude.util.logger
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
import io.ktor.server.request.header
import io.ktor.server.request.uri
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.util.toByteArray
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.io.FileNotFoundException
import java.util.Base64

private lateinit var evaluationProxy: EvaluationProxy

fun main() {
    val log = logger("Service")

    /*
     * Load the evaluation proxy's configuration.
     *
     * The PROXY_CONFIG_FILE_PATH environment variable determines where to load the proxy configuration from. The
     * PROXY_PROJECTS_FILE_PATH environment variable determines where to load the project info from. This defaults
     * to the config file path if not set, which defaults to `/etc/evaluation-proxy-config.yaml`.
     */
    log.info("Accessing proxy configuration.")
    val proxyConfigFilePath = stringEnv("PROXY_CONFIG_FILE_PATH", "/etc/evaluation-proxy-config.yaml")!!
    val proxyProjectsFilePath = stringEnv("PROXY_PROJECTS_FILE_PATH", proxyConfigFilePath)!!
    val projectsFile = try {
        ProjectsFile.fromFile(proxyProjectsFilePath).also {
            log.info("Found projects file at $proxyProjectsFilePath")
        }
    } catch (file: FileNotFoundException) {
        log.info("Proxy projects file not found at $proxyProjectsFilePath, reading project from env.")
        ProjectsFile.fromEnv()
    }
    val configFile = try {
        ConfigurationFile.fromFile(proxyConfigFilePath).also {
            log.info("Found configuration file at $proxyConfigFilePath")
        }
    } catch (file: FileNotFoundException) {
        log.info("Proxy config file not found at $proxyConfigFilePath, reading configuration from env.")
        ConfigurationFile.fromEnv()
    }

    /*
     * Initialize and start the evaluation proxy.
     */
    evaluationProxy = EvaluationProxy(
        projectsFile.projects,
        configFile.configuration
    )

    /*
     * Start the server.
     */
    embeddedServer(
        factory = Netty,
        port = configFile.configuration.port,
        host = "0.0.0.0",
        module = Application::proxyServer
    ).start(wait = true)
}

fun Application.proxyServer() {

    runBlocking {
        evaluationProxy.start()
    }

    /*
     * Configure ktor plugins.
     */
    configureLogging()
    configureMetrics()
    install(ContentNegotiation) {
        json()
    }
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

    /*
     * Configure endpoints.
     */
    routing {
        get("/sdk/v2/flags") {
            val deployment = this.call.request.getDeploymentKey()
            val result = try {
                evaluationProxy.getSerializedFlagConfigs(deployment)
            } catch (e: HttpErrorResponseException) {
                call.respond(HttpStatusCode.fromValue(e.status), e.message)
                return@get
            }
            call.respond(result)
        }

        get("/sdk/v2/users/{userId}/cohorts") {
            val deployment = this.call.request.getDeploymentKey()
            val userId = this.call.parameters["userId"]
            val result = try {
                evaluationProxy.getSerializedCohortMembershipsForUser(deployment, userId)
            } catch (e: HttpErrorResponseException) {
                call.respond(HttpStatusCode.fromValue(e.status), e.message)
                return@get
            }
            call.respond(result)
        }

        // V2 evaluation endpoints

        get("/sdk/v2/vardata") {
            call.evaluate(evaluationProxy, ApplicationRequest::getUserFromHeader)
        }

        post("/sdk/v2/vardata") {
            call.evaluate(evaluationProxy, ApplicationRequest::getUserFromBody)
        }

        // V1 evaluation endpoints

        get("/sdk/vardata") {
            call.evaluateV1(evaluationProxy, ApplicationRequest::getUserFromHeader)
        }

        post("/sdk/vardata") {
            call.evaluateV1(evaluationProxy, ApplicationRequest::getUserFromBody)
        }

        get("/v1/vardata") {
            call.evaluateV1(evaluationProxy, ApplicationRequest::getUserFromQuery)
        }

        post("/v1/vardata") {
            call.evaluateV1(evaluationProxy, ApplicationRequest::getUserFromBody)
        }

        // Health check

        get("/status") {
            call.respond("OK")
        }
    }
}

suspend fun ApplicationCall.evaluate(
    evaluationProxy: EvaluationProxy,
    userProvider: suspend ApplicationRequest.() -> Map<String, Any?>
) {
    val result = try {
        // Deployment key is included in Authorization header with prefix "Api-Key "
        val deploymentKey = request.getDeploymentKey()
        val user = request.userProvider()
        val flagKeys = request.getFlagKeys()
        evaluationProxy.serializedEvaluate(deploymentKey, user, flagKeys)
    } catch (e: HttpErrorResponseException) {
        respond(HttpStatusCode.fromValue(e.status), e.message)
        return
    }
    respond(result)
}

suspend fun ApplicationCall.evaluateV1(
    evaluationProxy: EvaluationProxy,
    userProvider: suspend ApplicationRequest.() -> Map<String, Any?>
) {
    val result = try {
        // Deployment key is included in Authorization header with prefix "Api-Key "
        val deploymentKey = request.getDeploymentKey()
        val user = request.userProvider()
        val flagKeys = request.getFlagKeys()
        evaluationProxy.serializedEvaluateV1(deploymentKey, user, flagKeys)
    } catch (e: HttpErrorResponseException) {
        respond(HttpStatusCode.fromValue(e.status), e.message)
        return
    }
    respond(result)
}

/**
 * Get the deployment key from the request, included in Authorization header with prefix "Api-Key "
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
private fun ApplicationRequest.getUserFromHeader(): Map<String, Any?> {
    val b64User = this.headers["X-Amp-Exp-User"]
    val userJson = Base64.getDecoder().decode(b64User).toString(Charsets.UTF_8)
    return json.decodeFromString(userJson)
}

/**
 * Get the user from the body. Used for SDK/REST POST requests.
 */
private suspend fun ApplicationRequest.getUserFromBody(): Map<String, Any?> {
    val userJson = this.receiveChannel().toByteArray().toString(Charsets.UTF_8)
    return json.decodeFromString(userJson)
}

/**
 * Get the user from the query. Used for REST GET requests.
 */
private fun ApplicationRequest.getUserFromQuery(): JsonObject {
    val userId = this.queryParameters["user_id"]
    val deviceId = this.queryParameters["device_id"]
    val context = this.queryParameters["context"]
    var user: JsonObject = if (context != null) {
        json.decodeFromString(context)
    } else {
        JsonObject(emptyMap())
    }
    if (userId != null) {
        user = JsonObject(user.toMutableMap().apply {
            put("user_id", JsonPrimitive(userId))
        })
    }
    if (deviceId != null) {
        user = JsonObject(user.toMutableMap().apply {
            put("device_id", JsonPrimitive(userId))
        })
    }
    return user
}
