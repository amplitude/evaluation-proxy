package com.amplitude.assignment

import com.amplitude.experiment.evaluation.FlagResult
import com.amplitude.experiment.evaluation.SkylabUser

const val DAY_MILLIS: Long = 24 * 60 * 60 * 1000

data class Assignment(
    val user: SkylabUser,
    val results: Map<String, FlagResult>,
    val timestamp: Long = System.currentTimeMillis(),
)

fun Assignment.canonicalize(): String {
    val sb = StringBuilder().append(this.user.userId?.trim(), " ", this.user.deviceId?.trim(), " ")
    for (key in this.results.keys.sorted()) {
        val value = this.results[key]
        sb.append(key.trim(), " ", value?.variant?.key?.trim(), " ")
    }
    return sb.toString()
}
