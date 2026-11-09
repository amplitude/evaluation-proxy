package com.amplitude.util

import com.amplitude.experiment.evaluation.EvaluationFlag

fun EvaluationFlag.getFlagVersion(): Long? {
    val value = this.metadata?.get("flagVersion")
    return when (value) {
        null -> null
        is Long -> value
        is Int -> value.toLong()
        is Double -> value.toLong()
        is Float -> value.toLong()
        is String -> value.toLongOrNull()
        is Number -> value.toLong()
        else -> null
    }
}

fun EvaluationFlag.isNewerThan(flag: EvaluationFlag?): Boolean {
    return (this.getFlagVersion() ?: Long.MAX_VALUE) > (flag?.getFlagVersion() ?: 0L)
}

/**
 * True if the flag is configured for remote evaluation. The flag server returns
 * such flags to local evaluation clients as a stub with no targeting rules.
 */
fun EvaluationFlag.isRemoteEvaluationMode(): Boolean {
    return (this.metadata?.get("evaluationMode") as? String).equals("remote", ignoreCase = true)
}
