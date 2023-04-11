package com.amplitude

import com.amplitude.util.booleanEnv
import com.amplitude.util.intEnv
import com.amplitude.util.longEnv
import com.amplitude.util.stringEnv

/**
 * Config Environment Variable Keys
 */
object EnvKey {
    const val FLAG_SYNC_INTERVAL_MILLIS = "AMPLITUDE_FLAG_SYNC_INTERVAL_MILLIS"
    const val COHORT_SYNC_INTERVAL_MILLIS = "AMPLITUDE_FLAG_SYNC_INTERVAL_MILLIS"
    const val MAX_COHORT_SIZE = "AMPLITUDE_MAX_COHORT_SIZE"

    const val ASSIGNMENT_FILTER_CAPACITY = "AMPLITUDE_ASSIGNMENT_FILTER_CAPACITY"
    const val ASSIGNMENT_EVENT_UPLOAD_THRESHOLD = "AMPLITUDE_ASSIGNMENT_EVENT_UPLOAD_THRESHOLD"
    const val ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS = "AMPLITUDE_ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS"
    const val ASSIGNMENT_USE_BATCH_MODE = "AMPLITUDE_ASSIGNMENT_USE_BATCH_MODE"

    const val REDIS_URL = "AMPLITUDE_REDIS_URL"
    const val REDIS_PREFIX = "AMPLITUDE_REDIS_PREFIX"
}

/**
 * Config Defaults
 */
object Default {
    const val FLAG_SYNC_INTERVAL_MILLIS = 10 * 1000L
    const val COHORT_SYNC_INTERVAL_MILLIS = 60 * 1000L
    const val MAX_COHORT_SIZE = Int.MAX_VALUE

    const val ASSIGNMENT_FILTER_CAPACITY = 1 shl 20
    const val ASSIGNMENT_EVENT_UPLOAD_THRESHOLD = 100
    const val ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS = 10000
    const val ASSIGNMENT_USE_BATCH_MODE = true

    val REDIS_URL: String? = null
    const val REDIS_PREFIX = "amplitude"
}

data class Configuration(
    val flagSyncIntervalMillis: Long = Default.FLAG_SYNC_INTERVAL_MILLIS,
    val cohortSyncIntervalMillis: Long = Default.COHORT_SYNC_INTERVAL_MILLIS,
    val maxCohortSize: Int = Default.MAX_COHORT_SIZE,
    val assignmentConfiguration: AssignmentConfiguration = AssignmentConfiguration(),
    val redisConfiguration: RedisConfiguration? = null
) {
    companion object {
        fun fromEnv() = Configuration(
            flagSyncIntervalMillis = longEnv(
                EnvKey.FLAG_SYNC_INTERVAL_MILLIS,
                Default.FLAG_SYNC_INTERVAL_MILLIS
            )!!,
            cohortSyncIntervalMillis = longEnv(
                EnvKey.COHORT_SYNC_INTERVAL_MILLIS,
                Default.COHORT_SYNC_INTERVAL_MILLIS
            )!!,
            maxCohortSize = intEnv(EnvKey.MAX_COHORT_SIZE, Default.MAX_COHORT_SIZE)!!,
            assignmentConfiguration = AssignmentConfiguration.fromEnv(),
            redisConfiguration = RedisConfiguration.fromEnv()
        )
    }
}

data class AssignmentConfiguration(
    val filterCapacity: Int = Default.ASSIGNMENT_FILTER_CAPACITY,
    val eventUploadThreshold: Int = Default.ASSIGNMENT_EVENT_UPLOAD_THRESHOLD,
    val eventUploadPeriodMillis: Int = Default.ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS,
    val useBatchMode: Boolean = Default.ASSIGNMENT_USE_BATCH_MODE
) {
    companion object {
        fun fromEnv() = AssignmentConfiguration(
            filterCapacity = intEnv(
                EnvKey.ASSIGNMENT_FILTER_CAPACITY,
                Default.ASSIGNMENT_FILTER_CAPACITY
            )!!,
            eventUploadThreshold = intEnv(
                EnvKey.ASSIGNMENT_EVENT_UPLOAD_THRESHOLD,
                Default.ASSIGNMENT_EVENT_UPLOAD_THRESHOLD
            )!!,
            eventUploadPeriodMillis = intEnv(
                EnvKey.ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS,
                Default.ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS
            )!!,
            useBatchMode = booleanEnv(
                EnvKey.ASSIGNMENT_USE_BATCH_MODE,
                Default.ASSIGNMENT_USE_BATCH_MODE
            )
        )
    }
}

data class RedisConfiguration(
    val redisUrl: String,
    val redisPrefix: String,
) {
    companion object {
        fun fromEnv(): RedisConfiguration? {
            val redisUrl = stringEnv(EnvKey.REDIS_URL, Default.REDIS_URL)
            val redisPrefix = stringEnv(EnvKey.REDIS_PREFIX, Default.REDIS_PREFIX)!!
            if (redisUrl != null) {
                return RedisConfiguration(redisUrl = redisUrl, redisPrefix = redisPrefix)
            }
            return null
        }
    }
}
