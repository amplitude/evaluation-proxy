package com.amplitude.plugins

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
import org.slf4j.event.Level

/**
 * Install the logging plugin and sets the level for logging each call.
 */
fun Application.configureLogging() {
    install(CallLogging) {
        filter { call -> call.request.path().startsWith("/") }
        level = Level.DEBUG
    }
}

/**
 * Enable prometheus metrics from the /metrics endpoint.
 */
fun Application.configureMetrics() {
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
