package com.amplitude.util.redis

import com.amplitude.RedisConfiguration
import com.amplitude.util.logger
import io.lettuce.core.ReadFrom

/**
 * Container for Redis connections - primary for writes, readOnly for high-volume reads
 */
internal data class RedisConnections(
    val primary: Redis,
    val readOnly: Redis,
)

internal object RedisConnectionsLogger {
    val log by logger()
}

/**
 * Parse readFrom configuration string to Lettuce ReadFrom enum.
 * Supports: ANY, REPLICA_PREFERRED
 * Invalid values default to ANY with a warning.
 */
internal fun parseReadFrom(readFromStr: String): ReadFrom {
    return when (readFromStr.uppercase()) {
        "ANY" -> ReadFrom.ANY
        "REPLICA_PREFERRED" -> ReadFrom.REPLICA_PREFERRED
        else -> {
            RedisConnectionsLogger.log.warn(
                "Invalid readFrom value: '$readFromStr'. " +
                    "Supported values: ANY, REPLICA_PREFERRED. Defaulting to ANY.",
            )
            ReadFrom.ANY
        }
    }
}

/**
 * Creates Redis connections based on configuration.
 * Returns null if no Redis is configured (should use in-memory storage).
 */
internal fun createRedisConnections(redisConfiguration: RedisConfiguration?): RedisConnections? {
    return when {
        redisConfiguration == null -> null

        redisConfiguration.useCluster && !redisConfiguration.uri.isNullOrBlank() -> {
            val readFromStrategy = parseReadFrom(redisConfiguration.readFrom)

            val redis =
                RedisClusterConnection(
                    redisConfiguration.uri,
                    redisConfiguration.connectionTimeoutMillis,
                    redisConfiguration.commandTimeoutMillis,
                    null,
                )
            // Create a dedicated read-only client using replicas only for read isolation
            val readOnlyRedis =
                RedisClusterConnection(
                    redisConfiguration.readOnlyUri ?: redisConfiguration.uri,
                    redisConfiguration.connectionTimeoutMillis,
                    redisConfiguration.commandTimeoutMillis,
                    readFromStrategy,
                )
            RedisConnections(redis, readOnlyRedis)
        }

        redisConfiguration.uri != null -> {
            val redis =
                RedisConnection(
                    redisConfiguration.uri,
                    redisConfiguration.connectionTimeoutMillis,
                    redisConfiguration.commandTimeoutMillis,
                )
            val readOnlyRedis =
                if (redisConfiguration.readOnlyUri != null) {
                    RedisConnection(
                        redisConfiguration.readOnlyUri,
                        redisConfiguration.connectionTimeoutMillis,
                        redisConfiguration.commandTimeoutMillis,
                    )
                } else {
                    redis
                }
            RedisConnections(redis, readOnlyRedis)
        }

        else -> null
    }
}
