package com.amplitude.plugins

import com.amplitude.util.booleanEnv
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.plugins.callloging.CallLogging
import io.ktor.server.request.path
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry

/**
 * Install the logging plugin and sets the level for logging each call.
 */
fun Application.configureLogging() {
    install(CallLogging) {
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
    if (booleanEnv("AMPLITUDE_METRICS")) {
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
