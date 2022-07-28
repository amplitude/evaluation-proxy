package com.amplitude.plugins

import com.amplitude.experiment.ExperimentUser
import com.amplitude.experiment.LocalEvaluationClient
import com.amplitude.experiment.evaluation.serialization.SerialExperimentUser
import com.amplitude.util.toExperimentUser
import com.amplitude.util.toSerialVariant
import io.ktor.server.routing.*
import io.ktor.server.application.*
import io.ktor.server.response.respond
import io.ktor.util.toByteArray
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import java.util.Base64

private val json = Json {
    ignoreUnknownKeys = true
}

fun Application.configureRouting(experiment: LocalEvaluationClient) {

    routing {
        get("/sdk/vardata") {
            val b64User = call.request.headers["X-Amp-Exp-User"]
            val userJson = Base64.getDecoder().decode(b64User).toString(Charsets.UTF_8)
            val serialUser: SerialExperimentUser = json.decodeFromString(userJson)
            val user = serialUser.toExperimentUser()
            val result = experiment.evaluate(user).mapValues { it.value.toSerialVariant() }
            call.respond(result)
        }
        post("/sdk/vardata") {
            val userJson = call.request.receiveChannel().toByteArray().toString(Charsets.UTF_8)
            val serialUser: SerialExperimentUser = json.decodeFromString(userJson)
            val user = serialUser.toExperimentUser()
            val result = experiment.evaluate(user).mapValues { it.value.toSerialVariant() }
            call.respond(result)
        }
        get("/v1/vardata") {
            val userId = call.request.queryParameters["user_id"]
            val deviceId = call.request.queryParameters["device_id"]
            val flagKey = call.request.queryParameters["flag_key"]
            val context = call.request.queryParameters["context"]
            val builder = if (context != null) {
                json.decodeFromString<SerialExperimentUser>(context).toExperimentUser().copyToBuilder()
            } else {
                ExperimentUser.builder()
            }
            if (userId != null) {
                builder.userId(userId)
            }
            if (deviceId != null) {
                builder.deviceId(deviceId)
            }
            val flagKeys = if (flagKey != null) {
                listOf(flagKey)
            } else {
                listOf()
            }
            val user = builder.build()
            val result = experiment.evaluate(user, flagKeys).mapValues { it.value.toSerialVariant() }
            call.respond(result)
        }
    }
}
