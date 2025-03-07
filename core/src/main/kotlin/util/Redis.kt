package com.amplitude.util

import com.amplitude.Metrics
import com.amplitude.RedisCommand
import com.amplitude.RedisCommandFailure
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.RedisURI
import io.lettuce.core.ScanArgs
import io.lettuce.core.ScanCursor
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.StringCodec
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.future.asDeferred
import kotlin.time.Duration

private const val STORAGE_PROTOCOL_VERSION = "v3"

internal sealed class RedisKey(val value: String) {
    data class Projects(val prefix: String) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects")

    data class Deployments(
        val prefix: String,
        val projectId: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:deployments")

    data class FlagConfigs(
        val prefix: String,
        val projectId: String,
        val deploymentKey: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:deployments:$deploymentKey:flags")

    data class CohortDescriptions(
        val prefix: String,
        val projectId: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:cohorts")

    data class CohortMembers(
        val prefix: String,
        val projectId: String,
        val cohortId: String,
        val cohortGroupType: String,
        val cohortLastModified: Long,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:cohorts:$cohortId:$cohortGroupType:$cohortLastModified")
}

internal interface Redis {
    suspend fun get(key: RedisKey): String?

    suspend fun set(
        key: RedisKey,
        value: String,
    )

    suspend fun del(key: RedisKey)

    suspend fun sadd(
        key: RedisKey,
        values: Set<String>,
    )

    suspend fun srem(
        key: RedisKey,
        value: String,
    )

    suspend fun sscan(
        key: RedisKey,
        limit: Long,
    ): Set<String>?

    suspend fun sismember(
        key: RedisKey,
        value: String,
    ): Boolean

    suspend fun hget(
        key: RedisKey,
        field: String,
    ): String?

    suspend fun hgetall(key: RedisKey): Map<String, String>?

    suspend fun hset(
        key: RedisKey,
        values: Map<String, String>,
    )

    suspend fun hdel(
        key: RedisKey,
        field: String,
    )

    suspend fun expire(
        key: RedisKey,
        ttl: Duration,
    )
}

internal class RedisConnection(
    redisUri: String,
) : Redis {
    private val connection: Deferred<StatefulRedisConnection<String, String>>
    private val client: RedisClient = RedisClient.create(redisUri)

    init {
        connection = client.connectAsync(StringCodec.UTF8, RedisURI.create(redisUri)).asDeferred()
    }

    override suspend fun get(key: RedisKey): String? {
        return connection.run {
            get(key.value)
        }
    }

    override suspend fun set(
        key: RedisKey,
        value: String,
    ) {
        connection.run {
            set(key.value, value)
        }
    }

    override suspend fun del(key: RedisKey) {
        connection.run {
            del(key.value)
        }
    }

    override suspend fun sadd(
        key: RedisKey,
        values: Set<String>,
    ) {
        connection.run {
            sadd(key.value, *values.toTypedArray())
        }
    }

    override suspend fun srem(
        key: RedisKey,
        value: String,
    ) {
        connection.run {
            srem(key.value, value)
        }
    }

    override suspend fun sscan(
        key: RedisKey,
        limit: Long,
    ): Set<String>? {
        var exists = connection.run { type(key.value) } != "none"
        if (!exists) {
            return null
        }
        val result = mutableSetOf<String>()
        var cursor = ScanCursor.INITIAL
        do {
            cursor =
                connection.run {
                    sscan(key.value, cursor, ScanArgs().limit(limit))
                }
            result.addAll(cursor.values)
        } while (!cursor.isFinished)
        exists = connection.run { type(key.value) } != "none"
        if (!exists) {
            // Set may expire or get deleted while the scan is in process.
            return null
        }
        return result
    }

    override suspend fun sismember(
        key: RedisKey,
        value: String,
    ): Boolean {
        return connection.run {
            sismember(key.value, value)
        }
    }

    override suspend fun hget(
        key: RedisKey,
        field: String,
    ): String? {
        return connection.run {
            hget(key.value, field)
        }
    }

    override suspend fun hgetall(key: RedisKey): Map<String, String>? {
        return connection.run {
            hgetall(key.value)
        }
    }

    override suspend fun hset(
        key: RedisKey,
        values: Map<String, String>,
    ) {
        connection.run {
            hset(key.value, values)
        }
    }

    override suspend fun hdel(
        key: RedisKey,
        field: String,
    ) {
        connection.run {
            hdel(key.value, field)
        }
    }

    override suspend fun expire(
        key: RedisKey,
        ttl: Duration,
    ) {
        connection.run {
            expire(key.value, ttl.inWholeSeconds)
        }
    }

    private suspend inline fun <reified R> Deferred<StatefulRedisConnection<String, String>>.run(
        crossinline action: RedisAsyncCommands<String, String>.() -> RedisFuture<R>,
    ): R {
        return Metrics.with({ RedisCommand }, { e -> RedisCommandFailure(e) }) {
            await().async().action().asDeferred().await()
        }
    }
}
