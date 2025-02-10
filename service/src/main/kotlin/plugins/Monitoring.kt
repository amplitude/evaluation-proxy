package com.amplitude.plugins

import com.amplitude.AssignmentEvent
import com.amplitude.AssignmentEventFilter
import com.amplitude.AssignmentEventSend
import com.amplitude.AssignmentEventSendFailure
import com.amplitude.CohortDownload
import com.amplitude.CohortDownloadFailure
import com.amplitude.DeploymentsFetch
import com.amplitude.DeploymentsFetchFailure
import com.amplitude.Evaluation
import com.amplitude.EvaluationFailure
import com.amplitude.EvaluationProxyEvaluationRequest
import com.amplitude.EvaluationProxyEvaluationRequestError
import com.amplitude.EvaluationProxyGetCohortRequest
import com.amplitude.EvaluationProxyGetCohortRequestError
import com.amplitude.EvaluationProxyGetFlagsRequest
import com.amplitude.EvaluationProxyGetFlagsRequestError
import com.amplitude.EvaluationProxyGetMembershipsRequest
import com.amplitude.EvaluationProxyGetMembershipsRequestError
import com.amplitude.EvaluationProxyRequest
import com.amplitude.EvaluationProxyRequestError
import com.amplitude.FailureMetric
import com.amplitude.FlagsFetch
import com.amplitude.FlagsFetchFailure
import com.amplitude.Metric
import com.amplitude.MetricsHandler
import com.amplitude.RedisCommand
import com.amplitude.RedisCommandFailure
import com.amplitude.util.logger
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.plugins.callloging.CallLogging
import io.ktor.server.request.path
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
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
fun Application.configureMetrics(metricsRegistry: PrometheusMeterRegistry) {
    install(MicrometerMetrics) {
        metricName = "amplitude_proxy_http_server_requests"
        registry = metricsRegistry
    }
}


class PrometheusMetrics(
    private val prometheus: PrometheusMeterRegistry,
    private val logFailures: Boolean,
) : MetricsHandler {

    companion object {
        val log by logger()
    }

    override fun track(metric: Metric) {
        when (metric) {
            is Evaluation -> {
                prometheus.counter("amplitude_proxy_evaluation_total").increment()
            }
            is EvaluationFailure -> {
                prometheus.counter("amplitude_proxy_evaluation_failure_total").increment()
            }
            is AssignmentEvent -> {
                prometheus.counter("amplitude_proxy_assignment_total").increment()
            }
            is AssignmentEventFilter -> {
                prometheus.counter("amplitude_proxy_assignment_filter_total").increment()
            }
            is AssignmentEventSend -> {
                prometheus.counter("amplitude_proxy_assignment_send_total").increment()
            }
            is AssignmentEventSendFailure -> {
                prometheus.counter("amplitude_proxy_assignment_send_failure_total").increment()
            }
            is DeploymentsFetch -> {
                prometheus.counter("amplitude_proxy_deployments_fetch_total").increment()
            }
            is DeploymentsFetchFailure -> {
                prometheus.counter("amplitude_proxy_deployments_fetch_failure_total").increment()
            }
            is FlagsFetch -> {
                prometheus.counter("amplitude_proxy_flags_fetch_total").increment()
            }
            is FlagsFetchFailure -> {
                prometheus.counter("amplitude_proxy_flags_fetch_failure_total").increment()
            }
            is CohortDownload -> {
                prometheus.counter("amplitude_proxy_cohort_download_total").increment()
            }
            is CohortDownloadFailure -> {
                prometheus.counter("amplitude_proxy_cohort_download_failure_total").increment()
            }
            is RedisCommand -> {
                prometheus.counter("amplitude_proxy_redis_command_total").increment()
            }
            is RedisCommandFailure -> {
                prometheus.counter("amplitude_proxy_redis_command_failure_total").increment()
            }
            is EvaluationProxyRequest -> {
                prometheus.counter("amplitude_proxy_request_total").increment()
            }
            is EvaluationProxyRequestError -> {
                prometheus.counter("amplitude_proxy_request_error_total").increment()
            }
            is EvaluationProxyGetFlagsRequest -> {
                prometheus.counter("amplitude_proxy_get_flags_request_total").increment()
            }
            is EvaluationProxyGetFlagsRequestError -> {
                prometheus.counter("amplitude_proxy_get_flags_request_error_total").increment()
            }
            is EvaluationProxyGetCohortRequest -> {
                prometheus.counter("amplitude_proxy_get_cohort_request_total").increment()
            }
            is EvaluationProxyGetCohortRequestError -> {
                prometheus.counter("amplitude_proxy_get_cohort_request_error_total").increment()
            }
            is EvaluationProxyGetMembershipsRequest -> {
                prometheus.counter("amplitude_proxy_get_members_request_total").increment()
            }
            is EvaluationProxyGetMembershipsRequestError -> {
                prometheus.counter("amplitude_proxy_get_members_request_error_total").increment()
            }
            is EvaluationProxyEvaluationRequest -> {
                prometheus.counter("amplitude_proxy_evaluation_request_total").increment()
            }
            is EvaluationProxyEvaluationRequestError -> {
                prometheus.counter("amplitude_proxy_evaluation_request_error_total").increment()
            }
        }
        if (logFailures && metric is FailureMetric) {
            log.error(metric.toString())
        }
    }
}
