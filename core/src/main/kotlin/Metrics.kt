package com.amplitude

sealed class Metric
sealed class FailureMetric : Metric()

interface MetricsHandler {
    fun track(metric: Metric)
}

data object Evaluation : Metric()
data class EvaluationFailure(val exception: Exception) : FailureMetric()
data object AssignmentEvent : Metric()
data object AssignmentEventFilter : Metric()
data object AssignmentEventSend : Metric()
data class AssignmentEventSendFailure(val exception: Exception) : FailureMetric()
data object DeploymentsFetch : Metric()
data class DeploymentsFetchFailure(val exception: Exception) : FailureMetric()
data object FlagsFetch : Metric()
data class FlagsFetchFailure(val exception: Exception) : FailureMetric()
data object CohortDescriptionFetch : Metric()
data class CohortDescriptionFetchFailure(val exception: Exception) : FailureMetric()
data object CohortDownload : Metric()
data class CohortDownloadFailure(val exception: Exception) : FailureMetric()
data object RedisCommand : Metric()
data class RedisCommandFailure(val exception: Exception) : FailureMetric()

internal object Metrics : MetricsHandler {

    internal var handler: MetricsHandler? = null

    override fun track(metric: Metric) {
        handler?.track(metric)
    }

    internal suspend fun <R> with(
        metric: (() -> Metric)?,
        failure: ((e: Exception) -> FailureMetric)?,
        block: suspend () -> R
    ): R {
        try {
            metric?.invoke()
            return block.invoke()
        } catch (e: Exception) {
            failure?.invoke(e)
            throw e
        }
    }
}
