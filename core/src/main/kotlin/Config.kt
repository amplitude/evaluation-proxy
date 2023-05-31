package com.amplitude

import com.amplitude.util.booleanEnv
import com.amplitude.util.intEnv
import com.amplitude.util.json
import com.amplitude.util.longEnv
import com.amplitude.util.stringEnv
import com.charleskorn.kaml.Yaml
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import java.io.File

@Serializable
data class ProjectsFile(
    val projects: List<Project>
) {
    companion object {
        fun fromEnv(): ProjectsFile {
            val project = Project.fromEnv()
            return ProjectsFile(listOf(project))
        }

        fun fromFile(path: String): ProjectsFile {
            val data = File(path).readText()
            return if (path.endsWith(".yaml") || path.endsWith(".yml")) {
                Yaml.default.decodeFromString(data)
            } else if (path.endsWith(".json")) {
                json.decodeFromString(data)
            } else {
                throw IllegalArgumentException("Proxy configuration file format must be \".yaml\" or \".yml\". Found $path")
            }
        }
    }
}

@Serializable
data class ConfigurationFile(
    val configuration: Configuration = Configuration()
) {
    companion object {

        fun fromEnv(): ConfigurationFile {
            val configuration = Configuration.fromEnv()
            return ConfigurationFile(configuration)
        }

        fun fromFile(path: String): ConfigurationFile {
            val data = File(path).readText()
            return if (path.endsWith(".yaml") || path.endsWith(".yml")) {
                Yaml.default.decodeFromString(data)
            } else if (path.endsWith(".json")) {
                json.decodeFromString(data)
            } else {
                throw IllegalArgumentException("Proxy configuration file format must be \".yaml\", \".yml\", or \".json\". Found $path")
            }
        }
    }
}

@Serializable
data class Project(
    val id: String,
    val apiKey: String,
    val secretKey: String,
    val deploymentKeys: Set<String>
) {
    companion object {
        fun fromEnv(): Project {
            val id = checkNotNull(stringEnv(EnvKey.PROJECT_ID)) { "${EnvKey.PROJECT_ID} environment variable must be set." }
            val apiKey = checkNotNull(stringEnv(EnvKey.API_KEY)) { "${EnvKey.API_KEY} environment variable must be set." }
            val secretKey = checkNotNull(stringEnv(EnvKey.SECRET_KEY)) { "${EnvKey.SECRET_KEY} environment variable must be set." }
            val deploymentKey = checkNotNull(stringEnv(EnvKey.EXPERIMENT_DEPLOYMENT_KEY)) { "${EnvKey.SECRET_KEY} environment variable must be set." }
            return Project(id, apiKey, secretKey, setOf(deploymentKey))
        }
    }
}

@Serializable
data class Configuration(
    val port: Int = Default.PORT,
    val serverUrl: String = Default.SERVER_URL,
    val cohortServerUrl: String = Default.COHORT_SERVER_URL,
    val flagSyncIntervalMillis: Long = Default.FLAG_SYNC_INTERVAL_MILLIS,
    val cohortSyncIntervalMillis: Long = Default.COHORT_SYNC_INTERVAL_MILLIS,
    val maxCohortSize: Int = Default.MAX_COHORT_SIZE,
    val assignment: AssignmentConfiguration = AssignmentConfiguration(),
    val redis: RedisConfiguration? = null
) {
    companion object {
        fun fromEnv() = Configuration(
            port = intEnv(EnvKey.PORT, Default.PORT)!!,
            serverUrl = stringEnv(EnvKey.SERVER_URL, Default.SERVER_URL)!!,
            cohortServerUrl = stringEnv(EnvKey.COHORT_SERVER_URL, Default.COHORT_SERVER_URL)!!,
            flagSyncIntervalMillis = longEnv(
                EnvKey.FLAG_SYNC_INTERVAL_MILLIS,
                Default.FLAG_SYNC_INTERVAL_MILLIS
            )!!,
            cohortSyncIntervalMillis = longEnv(
                EnvKey.COHORT_SYNC_INTERVAL_MILLIS,
                Default.COHORT_SYNC_INTERVAL_MILLIS
            )!!,
            maxCohortSize = intEnv(EnvKey.MAX_COHORT_SIZE, Default.MAX_COHORT_SIZE)!!,
            assignment = AssignmentConfiguration.fromEnv(),
            redis = RedisConfiguration.fromEnv()
        )
    }
}

@Serializable
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

@Serializable
data class RedisConfiguration(
    val uri: String? = null,
    val readOnlyUri: String? = uri,
    val prefix: String = Default.REDIS_PREFIX
) {
    companion object {
        fun fromEnv(): RedisConfiguration? {
            val redisUri = stringEnv(EnvKey.REDIS_URI, Default.REDIS_URI)
            return if (redisUri != null) {
                val redisReadOnlyUri = stringEnv(EnvKey.REDIS_READ_ONLY_URI, Default.REDIS_READ_ONLY_URI) ?: redisUri
                val redisPrefix = stringEnv(EnvKey.REDIS_PREFIX, Default.REDIS_PREFIX)!!
                RedisConfiguration(
                    uri = redisUri,
                    readOnlyUri = redisReadOnlyUri,
                    prefix = redisPrefix
                )
            } else {
                null
            }
        }
    }
}

object EnvKey {
    const val PORT = "AMPLITUDE_PORT"
    const val SERVER_URL = "AMPLITUDE_SERVER_URL"
    const val COHORT_SERVER_URL = "AMPLITUDE_COHORT_SERVER_URL"

    const val PROJECT_ID = "AMPLITUDE_PROJECT_ID"
    const val API_KEY = "AMPLITUDE_API_KEY"
    const val SECRET_KEY = "AMPLITUDE_SECRET_KEY"
    const val EXPERIMENT_DEPLOYMENT_KEY = "AMPLITUDE_EXPERIMENT_DEPLOYMENT_KEY"

    const val FLAG_SYNC_INTERVAL_MILLIS = "AMPLITUDE_FLAG_SYNC_INTERVAL_MILLIS"
    const val COHORT_SYNC_INTERVAL_MILLIS = "AMPLITUDE_COHORT_SYNC_INTERVAL_MILLIS"
    const val MAX_COHORT_SIZE = "AMPLITUDE_MAX_COHORT_SIZE"

    const val ASSIGNMENT_FILTER_CAPACITY = "AMPLITUDE_ASSIGNMENT_FILTER_CAPACITY"
    const val ASSIGNMENT_EVENT_UPLOAD_THRESHOLD = "AMPLITUDE_ASSIGNMENT_EVENT_UPLOAD_THRESHOLD"
    const val ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS = "AMPLITUDE_ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS"
    const val ASSIGNMENT_USE_BATCH_MODE = "AMPLITUDE_ASSIGNMENT_USE_BATCH_MODE"

    const val REDIS_URI = "AMPLITUDE_REDIS_URI"
    const val REDIS_READ_ONLY_URI = "AMPLITUDE_REDIS_READ_ONLY_URI"
    const val REDIS_PREFIX = "AMPLITUDE_REDIS_PREFIX"
}

object Default {
    const val PORT = 3546
    const val SERVER_URL = "https://api.lab.amplitude.com"
    const val COHORT_SERVER_URL = "https://cohort.lab.amplitude.com"
    const val FLAG_SYNC_INTERVAL_MILLIS = 10 * 1000L
    const val COHORT_SYNC_INTERVAL_MILLIS = 60 * 1000L
    const val MAX_COHORT_SIZE = Int.MAX_VALUE

    const val ASSIGNMENT_FILTER_CAPACITY = 1 shl 20
    const val ASSIGNMENT_EVENT_UPLOAD_THRESHOLD = 100
    const val ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS = 10000
    const val ASSIGNMENT_USE_BATCH_MODE = true

    val REDIS_URI: String? = null
    val REDIS_READ_ONLY_URI: String? = null
    const val REDIS_PREFIX = "amplitude"
}
