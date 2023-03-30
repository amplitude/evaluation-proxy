package com.amplitude.assignment

import com.amplitude.util.booleanEnv
import com.amplitude.util.intEnv

const val ENV_KEY_ASSIGNMENT_FILTER_CAPACITY = "AMPLITUDE_CONFIG_ASSIGNMENT_FILTER_CAPACITY"
const val ENV_KEY_ASSIGNMENT_EVENT_UPLOAD_THRESHOLD = "AMPLITUDE_CONFIG_ASSIGNMENT_EVENT_UPLOAD_THRESHOLD"
const val ENV_KEY_ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS = "AMPLITUDE_CONFIG_ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS"
const val ENV_KEY_ASSIGNMENT_USE_BATCH_MODE = "AMPLITUDE_CONFIG_ASSIGNMENT_USE_BATCH_MODE"

const val DEFAULT_FILTER_CAPACITY = 1 shl 20
const val DEFAULT_EVENT_UPLOAD_THRESHOLD = 100
const val DEFAULT_EVENT_UPLOAD_PERIOD_MILLIS = 10000
const val DEFAULT_USE_BATCH_MODE = true


data class AssignmentConfiguration(
    val filterCapacity: Int = DEFAULT_FILTER_CAPACITY,
    val eventUploadThreshold: Int = DEFAULT_EVENT_UPLOAD_THRESHOLD,
    val eventUploadPeriodMillis: Int = DEFAULT_EVENT_UPLOAD_PERIOD_MILLIS,
    val useBatchMode: Boolean = DEFAULT_USE_BATCH_MODE,
) {
    companion object {
        fun fromEnv() = AssignmentConfiguration(
            filterCapacity = intEnv(
                ENV_KEY_ASSIGNMENT_FILTER_CAPACITY,
                DEFAULT_FILTER_CAPACITY,
            )!!,
            eventUploadThreshold = intEnv(
                ENV_KEY_ASSIGNMENT_EVENT_UPLOAD_THRESHOLD,
                DEFAULT_EVENT_UPLOAD_THRESHOLD
            )!!,
            eventUploadPeriodMillis = intEnv(
                ENV_KEY_ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS,
                DEFAULT_EVENT_UPLOAD_PERIOD_MILLIS,
            )!!,
            useBatchMode = booleanEnv(
                ENV_KEY_ASSIGNMENT_USE_BATCH_MODE,
                DEFAULT_USE_BATCH_MODE,
            )
        )
    }
}
