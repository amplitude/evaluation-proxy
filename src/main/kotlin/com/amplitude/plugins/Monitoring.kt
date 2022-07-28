package com.amplitude.plugins

import com.amplitude.util.booleanEnv
import com.amplitude.util.stringEnv
import io.ktor.server.plugins.callloging.*
import org.slf4j.event.*
import io.ktor.server.request.*
import io.micrometer.prometheus.*
import io.ktor.server.metrics.micrometer.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

/**
 * Install the logging plugin and sets the level for logging each call.
 *
 *  - Env: `EVALUATION_LOG_LEVEL`
 *  - Values: `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`
 */
fun Application.configureLogging() {

    val logLevel = try {
        Level.valueOf(stringEnv("EVALUATION_LOG_LEVEL") ?: "INFO")
    } catch (_: Exception) {
        Level.INFO
    }
    install(CallLogging) {
        level = logLevel
        filter { call -> call.request.path().startsWith("/") }
    }
}
/**
 * Enable or disable prometheus metrics from the /metrics endpoint.
 *
 *  - Env: EVALUATION_METRICS
 *  - Value: true, false
 */
fun Application.configureMetrics() {
    if (booleanEnv("EVALUATION_METRICS")) {
        val metricsRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        install(MicrometerMetrics) {
            metricName = "evaluation.http.server.requests"
            registry = metricsRegistry
        }
        routing {
            get("/metrics") {
                call.respond(metricsRegistry.scrape())
            }
        }
    }
}
