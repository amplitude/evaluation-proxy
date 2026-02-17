package com.amplitude.exposure

import com.amplitude.experiment.evaluation.EvaluationContext
import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.util.deviceId
import com.amplitude.util.userId

internal const val DAY_MILLIS: Long = 24 * 60 * 60 * 1000

internal data class Exposure(
    val context: EvaluationContext,
    val results: Map<String, EvaluationVariant>,
    val timestamp: Long = System.currentTimeMillis(),
)

internal fun Exposure.canonicalize(): String {
    val sb = StringBuilder().append(this.context.userId()?.trim(), " ", this.context.deviceId()?.trim(), " ")
    for (key in this.results.keys.sorted()) {
        val variant = this.results[key]
        sb.append(key.trim(), " ", variant?.key?.trim(), " ")
    }
    return sb.toString()
}
