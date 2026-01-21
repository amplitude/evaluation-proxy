package com.amplitude

import com.amplitude.util.booleanEnv
import com.amplitude.util.intEnv
import com.amplitude.util.json
import com.amplitude.util.longEnv
import com.amplitude.util.stringEnv
import com.amplitude.util.yaml
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import java.io.File

@Serializable
data class ProjectsFile(
    val projects: List<ProjectConfiguration>,
) {
    companion object {
        fun fromEnv(): ProjectsFile {
            val project = ProjectConfiguration.fromEnv()
            return ProjectsFile(listOf(project))
        }

        fun fromFile(path: String): ProjectsFile {
            val data = File(path).readText()
            return if (path.endsWith(".yaml") || path.endsWith(".yml")) {
                yaml.decodeFromString(data)
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
    val configuration: Configuration = Configuration(),
) {
    companion object {
        fun fromEnv(): ConfigurationFile {
            val configuration = Configuration.fromEnv()
            return ConfigurationFile(configuration)
        }

        fun fromFile(path: String): ConfigurationFile {
            val data = File(path).readText()
            return if (path.endsWith(".yaml") || path.endsWith(".yml")) {
                yaml.decodeFromString(data)
            } else if (path.endsWith(".json")) {
                json.decodeFromString(data)
            } else {
                throw IllegalArgumentException("Proxy configuration file format must be \".yaml\", \".yml\", or \".json\". Found $path")
            }
        }
    }
}

@Serializable
data class ProjectConfiguration(
    val apiKey: String,
    val secretKey: String,
    val managementKey: String,
) {
    companion object {
        fun fromEnv(): ProjectConfiguration {
            val apiKey = checkNotNull(stringEnv(EnvKey.API_KEY)) { "${EnvKey.API_KEY} environment variable must be set." }
            val secretKey = checkNotNull(stringEnv(EnvKey.SECRET_KEY)) { "${EnvKey.SECRET_KEY} environment variable must be set." }
            val managementKey =
                checkNotNull(
                    stringEnv(EnvKey.EXPERIMENT_MANAGEMENT_KEY),
                ) { "${EnvKey.EXPERIMENT_MANAGEMENT_KEY} environment variable must be set." }
            return ProjectConfiguration(apiKey, secretKey, managementKey)
        }
    }
}

@Serializable
data class Configuration(
    val port: Int = Default.PORT,
    val serverZone: String = Default.SERVER_ZONE,
    val serverUrl: String = getServerUrl(serverZone),
    val cohortServerUrl: String = getCohortServerUrl(serverZone),
    val managementServerUrl: String = getManagementServerUrl(serverZone),
    val analyticsServerUrl: String = getAnalyticsServerUrl(serverZone),
    val deploymentSyncIntervalMillis: Long = Default.DEPLOYMENT_SYNC_INTERVAL_MILLIS,
    val flagSyncIntervalMillis: Long = Default.FLAG_SYNC_INTERVAL_MILLIS,
    val cohortSyncIntervalMillis: Long = Default.COHORT_SYNC_INTERVAL_MILLIS,
    val maxCohortSize: Int = Default.MAX_COHORT_SIZE,
    val assignment: AssignmentConfiguration = AssignmentConfiguration(),
    val redis: RedisConfiguration? = null,
    val metrics: MetricsConfiguration = MetricsConfiguration(),
) {
    companion object {
        fun fromEnv() =
            Configuration(
                port = intEnv(EnvKey.PORT, Default.PORT)!!,
                serverZone = stringEnv(EnvKey.SERVER_ZONE, Default.SERVER_ZONE)!!,
                serverUrl = stringEnv(EnvKey.SERVER_URL, Default.US_SERVER_URL)!!,
                cohortServerUrl = stringEnv(EnvKey.COHORT_SERVER_URL, Default.US_COHORT_SERVER_URL)!!,
                managementServerUrl = stringEnv(EnvKey.MANAGEMENT_SERVER_URL, Default.US_MANAGEMENT_SERVER_URL)!!,
                analyticsServerUrl = stringEnv(EnvKey.ANALYTICS_SERVER_URL, Default.US_ANALYTICS_SERVER_URL)!!,
                deploymentSyncIntervalMillis =
                    longEnv(
                        EnvKey.DEPLOYMENT_SYNC_INTERVAL_MILLIS,
                        Default.DEPLOYMENT_SYNC_INTERVAL_MILLIS,
                    )!!,
                flagSyncIntervalMillis =
                    longEnv(
                        EnvKey.FLAG_SYNC_INTERVAL_MILLIS,
                        Default.FLAG_SYNC_INTERVAL_MILLIS,
                    )!!,
                cohortSyncIntervalMillis =
                    longEnv(
                        EnvKey.COHORT_SYNC_INTERVAL_MILLIS,
                        Default.COHORT_SYNC_INTERVAL_MILLIS,
                    )!!,
                maxCohortSize = intEnv(EnvKey.MAX_COHORT_SIZE, Default.MAX_COHORT_SIZE)!!,
                assignment = AssignmentConfiguration.fromEnv(),
                redis = RedisConfiguration.fromEnv(),
            )
    }
}

@Serializable
data class AssignmentConfiguration(
    val filterCapacity: Int = Default.ASSIGNMENT_FILTER_CAPACITY,
    val eventUploadThreshold: Int = Default.ASSIGNMENT_EVENT_UPLOAD_THRESHOLD,
    val eventUploadPeriodMillis: Int = Default.ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS,
    val useBatchMode: Boolean = Default.ASSIGNMENT_USE_BATCH_MODE,
) {
    companion object {
        fun fromEnv() =
            AssignmentConfiguration(
                filterCapacity =
                    intEnv(
                        EnvKey.ASSIGNMENT_FILTER_CAPACITY,
                        Default.ASSIGNMENT_FILTER_CAPACITY,
                    )!!,
                eventUploadThreshold =
                    intEnv(
                        EnvKey.ASSIGNMENT_EVENT_UPLOAD_THRESHOLD,
                        Default.ASSIGNMENT_EVENT_UPLOAD_THRESHOLD,
                    )!!,
                eventUploadPeriodMillis =
                    intEnv(
                        EnvKey.ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS,
                        Default.ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS,
                    )!!,
                useBatchMode =
                    booleanEnv(
                        EnvKey.ASSIGNMENT_USE_BATCH_MODE,
                        Default.ASSIGNMENT_USE_BATCH_MODE,
                    ),
            )
    }
}

@Serializable
data class RedisConfiguration(
    val uri: String? = null,
    val readOnlyUri: String? = uri,
    val useCluster: Boolean = false,
    val readFrom: String = Default.REDIS_READ_FROM,
    val prefix: String = Default.REDIS_PREFIX,
    val scanLimit: Long = Default.REDIS_SCAN_LIMIT,
    val connectionTimeoutMillis: Long = Default.REDIS_CONNECTION_TIMEOUT_MILLIS,
    val commandTimeoutMillis: Long = Default.REDIS_COMMAND_TIMEOUT_MILLIS,
    val pipelineBatchSize: Int = Default.REDIS_PIPELINE_BATCH_SIZE,
) {
    companion object {
        fun fromEnv(): RedisConfiguration? {
            val redisUri = stringEnv(EnvKey.REDIS_URI, Default.REDIS_URI)
            val useCluster = booleanEnv(EnvKey.REDIS_USE_CLUSTER, Default.REDIS_USE_CLUSTER)

            return if (redisUri != null) {
                val redisReadOnlyUri = stringEnv(EnvKey.REDIS_READ_ONLY_URI, Default.REDIS_READ_ONLY_URI) ?: redisUri
                val redisReadFrom = stringEnv(EnvKey.REDIS_READ_FROM, Default.REDIS_READ_FROM)!!
                val redisPrefix = stringEnv(EnvKey.REDIS_PREFIX, Default.REDIS_PREFIX)!!
                val redisScanLimit = longEnv(EnvKey.REDIS_SCAN_LIMIT, Default.REDIS_SCAN_LIMIT)!!
                val connectionTimeoutMillis = longEnv(EnvKey.REDIS_CONNECTION_TIMEOUT_MILLIS, Default.REDIS_CONNECTION_TIMEOUT_MILLIS)!!
                val commandTimeoutMillis = longEnv(EnvKey.REDIS_COMMAND_TIMEOUT_MILLIS, Default.REDIS_COMMAND_TIMEOUT_MILLIS)!!
                val pipelineBatchSize = intEnv(EnvKey.REDIS_PIPELINE_BATCH_SIZE, Default.REDIS_PIPELINE_BATCH_SIZE)!!

                RedisConfiguration(
                    uri = redisUri,
                    readOnlyUri = redisReadOnlyUri,
                    useCluster = useCluster,
                    readFrom = redisReadFrom,
                    prefix = redisPrefix,
                    scanLimit = redisScanLimit,
                    connectionTimeoutMillis = connectionTimeoutMillis,
                    commandTimeoutMillis = commandTimeoutMillis,
                    pipelineBatchSize = pipelineBatchSize,
                )
            } else {
                null
            }
        }
    }
}

@Serializable
data class MetricsConfiguration(
    val port: Int = Default.METRICS_PORT,
    val path: String = Default.METRICS_PATH,
    val logFailures: Boolean = Default.METRICS_LOG_FAILURES,
) {
    companion object {
        fun fromEnv(): MetricsConfiguration {
            val port = intEnv(EnvKey.METRICS_PORT, Default.METRICS_PORT)!!
            val path = stringEnv(EnvKey.METRICS_PATH, Default.METRICS_PATH)!!
            val logFailures = booleanEnv(EnvKey.METRICS_LOG_FAILURES, Default.METRICS_LOG_FAILURES)
            return MetricsConfiguration(
                port = port,
                path = path,
                logFailures = logFailures,
            )
        }
    }
}

object EnvKey {
    const val PORT = "AMPLITUDE_PORT"
    const val SERVER_ZONE = "AMPLITUDE_SERVER_ZONE"
    const val SERVER_URL = "AMPLITUDE_SERVER_URL"
    const val COHORT_SERVER_URL = "AMPLITUDE_COHORT_SERVER_URL"
    const val MANAGEMENT_SERVER_URL = "AMPLITUDE_MANAGEMENT_SERVER_URL"
    const val ANALYTICS_SERVER_URL = "AMPLITUDE_ANALYTICS_SERVER_URL"

    const val API_KEY = "AMPLITUDE_API_KEY"
    const val SECRET_KEY = "AMPLITUDE_SECRET_KEY"
    const val EXPERIMENT_MANAGEMENT_KEY = "AMPLITUDE_EXPERIMENT_MANAGEMENT_API_KEY"

    const val DEPLOYMENT_SYNC_INTERVAL_MILLIS = "AMPLITUDE_DEPLOYMENT_SYNC_INTERVAL_MILLIS"
    const val FLAG_SYNC_INTERVAL_MILLIS = "AMPLITUDE_FLAG_SYNC_INTERVAL_MILLIS"
    const val COHORT_SYNC_INTERVAL_MILLIS = "AMPLITUDE_COHORT_SYNC_INTERVAL_MILLIS"
    const val MAX_COHORT_SIZE = "AMPLITUDE_MAX_COHORT_SIZE"

    const val ASSIGNMENT_FILTER_CAPACITY = "AMPLITUDE_ASSIGNMENT_FILTER_CAPACITY"
    const val ASSIGNMENT_EVENT_UPLOAD_THRESHOLD = "AMPLITUDE_ASSIGNMENT_EVENT_UPLOAD_THRESHOLD"
    const val ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS = "AMPLITUDE_ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS"
    const val ASSIGNMENT_USE_BATCH_MODE = "AMPLITUDE_ASSIGNMENT_USE_BATCH_MODE"

    const val REDIS_URI = "AMPLITUDE_REDIS_URI"
    const val REDIS_READ_ONLY_URI = "AMPLITUDE_REDIS_READ_ONLY_URI"
    const val REDIS_USE_CLUSTER = "AMPLITUDE_REDIS_USE_CLUSTER"
    const val REDIS_READ_FROM = "AMPLITUDE_REDIS_READ_FROM"
    const val REDIS_PREFIX = "AMPLITUDE_REDIS_PREFIX"
    const val REDIS_SCAN_LIMIT = "AMPLITUDE_REDIS_SCAN_LIMIT"
    const val REDIS_CONNECTION_TIMEOUT_MILLIS = "AMPLITUDE_REDIS_CONNECTION_TIMEOUT_MILLIS"
    const val REDIS_COMMAND_TIMEOUT_MILLIS = "AMPLITUDE_REDIS_COMMAND_TIMEOUT_MILLIS"
    const val REDIS_PIPELINE_BATCH_SIZE = "AMPLITUDE_REDIS_PIPELINE_BATCH_SIZE"

    const val METRICS_PORT = "AMPLITUDE_METRICS_PORT"
    const val METRICS_PATH = "AMPLITUDE_METRICS_PATH"
    const val METRICS_LOG_FAILURES = "AMPLITUDE_METRICS_LOG_FAILURES"
}

object Default {
    const val PORT = 3546
    const val SERVER_ZONE = "US"
    const val US_SERVER_URL = "https://flag.lab.amplitude.com"
    const val US_COHORT_SERVER_URL = "https://cohort-v2.lab.amplitude.com"
    const val US_MANAGEMENT_SERVER_URL = "https://experiment.amplitude.com"
    const val US_ANALYTICS_SERVER_URL = "https://api2.amplitude.com/2/httpapi"
    const val EU_SERVER_URL = "https://flag.lab.eu.amplitude.com"
    const val EU_COHORT_SERVER_URL = "https://cohort-v2.lab.eu.amplitude.com"
    const val EU_MANAGEMENT_SERVER_URL = "https://experiment.eu.amplitude.com"
    const val EU_ANALYTICS_SERVER_URL = "https://api.eu.amplitude.com/2/httpapi"
    const val DEPLOYMENT_SYNC_INTERVAL_MILLIS = 60 * 2 * 1000L
    const val FLAG_SYNC_INTERVAL_MILLIS = 60 * 1000L
    const val COHORT_SYNC_INTERVAL_MILLIS = 60 * 2 * 1000L
    const val MAX_COHORT_SIZE = Int.MAX_VALUE

    const val ASSIGNMENT_FILTER_CAPACITY = 1 shl 20
    const val ASSIGNMENT_EVENT_UPLOAD_THRESHOLD = 100
    const val ASSIGNMENT_EVENT_UPLOAD_PERIOD_MILLIS = 10000
    const val ASSIGNMENT_USE_BATCH_MODE = true

    val REDIS_URI: String? = null
    val REDIS_READ_ONLY_URI: String? = null
    const val REDIS_USE_CLUSTER = false
    const val REDIS_READ_FROM = "ANY"
    const val REDIS_PREFIX = "amplitude"
    const val REDIS_SCAN_LIMIT = 10000L
    const val REDIS_CONNECTION_TIMEOUT_MILLIS = 10000L
    const val REDIS_COMMAND_TIMEOUT_MILLIS = 10000L
    const val REDIS_PIPELINE_BATCH_SIZE = 100

    const val METRICS_PORT = 9090
    const val METRICS_PATH = "metrics"
    const val METRICS_LOG_FAILURES = false
}

private fun getServerUrl(zone: String): String {
    return if (zone == "EU") {
        Default.EU_SERVER_URL
    } else {
        Default.US_SERVER_URL
    }
}

private fun getCohortServerUrl(zone: String): String {
    return if (zone == "EU") {
        Default.EU_COHORT_SERVER_URL
    } else {
        Default.US_COHORT_SERVER_URL
    }
}

private fun getManagementServerUrl(zone: String): String {
    return if (zone == "EU") {
        Default.EU_MANAGEMENT_SERVER_URL
    } else {
        Default.US_MANAGEMENT_SERVER_URL
    }
}

private fun getAnalyticsServerUrl(zone: String): String {
    return if (zone == "EU") {
        Default.EU_ANALYTICS_SERVER_URL
    } else {
        Default.US_ANALYTICS_SERVER_URL
    }
}
