package project

import com.amplitude.util.intEnv
import com.amplitude.util.longEnv

const val ENV_KEY_SYNC_INTERVAL_MILLIS = "AMPLITUDE_CONFIG_SYNC_INTERVAL_MILLIS"
const val ENV_KEY_MAX_COHORT_SIZE = "AMPLITUDE_CONFIG_MAX_COHORT_SIZE"

const val DEFAULT_SYNC_INTERVAL_MILLIS = 10 * 1000L
const val DEFAULT_MAX_COHORT_SIZE = Int.MAX_VALUE

data class ProjectConfiguration(
    val syncIntervalMillis: Long = DEFAULT_SYNC_INTERVAL_MILLIS,
    val maxCohortSize: Int = DEFAULT_MAX_COHORT_SIZE
) {
    companion object {
        fun fromEnv() = ProjectConfiguration(
            syncIntervalMillis = longEnv(
                ENV_KEY_SYNC_INTERVAL_MILLIS,
                DEFAULT_SYNC_INTERVAL_MILLIS
            )!!,
            maxCohortSize = intEnv(ENV_KEY_MAX_COHORT_SIZE, DEFAULT_MAX_COHORT_SIZE)!!
        )
    }
}
