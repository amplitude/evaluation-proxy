package com.amplitude.util.redis

import com.amplitude.RedisConfiguration

/**
 * Container for Redis connections - primary for writes, readOnly for high-volume reads
 */
internal data class RedisConnections(
    val primary: Redis,
    val readOnly: Redis,
)

/**
 * Creates Redis connections based on configuration.
 * Returns null if no Redis is configured (should use in-memory storage).
 */
internal fun createRedisConnections(redisConfiguration: RedisConfiguration?): RedisConnections? {
    return when {
        redisConfiguration == null -> null
        
        redisConfiguration.useCluster && !redisConfiguration.uri.isNullOrBlank() -> {
            val redis = RedisClusterConnection(
                redisConfiguration.uri,
                redisConfiguration.connectionTimeoutMillis,
                redisConfiguration.commandTimeoutMillis,
            )
            // For cluster mode, read-only replica is not typically configured separately
            // All nodes in cluster can handle reads, so we use the same connection
            val readOnlyRedis = redis
            RedisConnections(redis, readOnlyRedis)
        }
        
        redisConfiguration.uri != null -> {
            val redis = RedisConnection(
                redisConfiguration.uri,
                redisConfiguration.connectionTimeoutMillis,
                redisConfiguration.commandTimeoutMillis,
            )
            val readOnlyRedis = if (redisConfiguration.readOnlyUri != null) {
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
