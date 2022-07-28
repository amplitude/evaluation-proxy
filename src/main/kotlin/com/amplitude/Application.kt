package com.amplitude

import com.amplitude.experiment.Experiment
import com.amplitude.experiment.LocalEvaluationConfig
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import com.amplitude.plugins.*
import com.amplitude.util.longEnv
import com.amplitude.util.stringEnv

const val DEFAULT_HOST = "0.0.0.0"
const val DEFAULT_PORT = 3546

fun main() {
    val deploymentKey = validateDeploymentKey(stringEnv("EVALUATION_DEPLOYMENT_KEY"))
    val config = getSdkConfiguration()
    val experiment = Experiment.initializeLocal(deploymentKey, config)
    experiment.start()
    embeddedServer(Netty, port = DEFAULT_PORT, host = DEFAULT_HOST) {
        configureLogging()
        configureMetrics()
        configureSerialization()
        configureAdministration()
        configureRouting(experiment)
    }.start(wait = true)
}

private fun validateDeploymentKey(key: String?): String {
    checkNotNull(key) {
        "The EVALUATION_DEPLOYMENT_KEY environment variable must be set to your server-side experiment deployment key"
    }
    if (!key.startsWith("server-")) {
        throw IllegalArgumentException(
            "The EVALUATION_DEPLOYMENT_KEY environment variable value must be a server-side deployment key (found \"$key\""
        )
    }
    return key
}

private fun getSdkConfiguration(): LocalEvaluationConfig {
    val pollingIntervalMillis = longEnv("EVALUATION_POLLING_INTERVAL_MILLIS")
    val builder = LocalEvaluationConfig.builder()
    if (pollingIntervalMillis != null) {
        builder.flagConfigPollerIntervalMillis(pollingIntervalMillis)
    }
    return builder.build()
}



