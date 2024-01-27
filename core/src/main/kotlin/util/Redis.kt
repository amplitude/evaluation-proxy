package com.amplitude.util

import com.amplitude.Metrics
import com.amplitude.RedisCommand
import com.amplitude.RedisCommandFailure
import com.amplitude.cohort.CohortDescription
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.StringCodec
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.future.asDeferred
import kotlin.time.Duration

private const val STORAGE_PROTOCOL_VERSION = "v2"

internal sealed class RedisKey(val value: String) {

    data class Projects(val prefix: String) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects")

    data class Deployments(
        val prefix: String,
        val projectId: String
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:deployments")

    data class FlagConfigs(
        val prefix: String,
        val projectId: String,
        val deploymentKey: String
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:deployments:$deploymentKey:flags")

    data class CohortDescriptions(
        val prefix: String,
        val projectId: String
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:cohorts")

    data class CohortMembers(
        val prefix: String,
        val projectId: String,
        val cohortDescription: CohortDescription
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:cohorts:${cohortDescription.id}:${cohortDescription.groupType}:${cohortDescription.lastComputed}")
}

internal interface Redis {
    suspend fun get(key: RedisKey): String?
    suspend fun set(key: RedisKey, value: String)
    suspend fun del(key: RedisKey)
    suspend fun sadd(key: RedisKey, values: Set<String>)
    suspend fun srem(key: RedisKey, value: String)
    suspend fun smembers(key: RedisKey): Set<String>?
    suspend fun sismember(key: RedisKey, value: String): Boolean
    suspend fun hget(key: RedisKey, field: String): String?
    suspend fun hgetall(key: RedisKey): Map<String, String>?
    suspend fun hset(key: RedisKey, values: Map<String, String>)
    suspend fun hdel(key: RedisKey, field: String)
    suspend fun expire(key: RedisKey, ttl: Duration)
}

internal class RedisConnection(
    redisUri: String
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

    override suspend fun set(key: RedisKey, value: String) {
        connection.run {
            set(key.value, value)
        }
    }

    override suspend fun del(key: RedisKey) {
        connection.run {
            del(key.value)
        }
    }

    override suspend fun sadd(key: RedisKey, values: Set<String>) {
        connection.run {
            sadd(key.value, *values.toTypedArray())
        }
    }

    override suspend fun srem(key: RedisKey, value: String) {
        connection.run {
            srem(key.value, value)
        }
    }

    override suspend fun smembers(key: RedisKey): Set<String>? {
        return connection.run {
            smembers(key.value)
        }
    }

    override suspend fun sismember(key: RedisKey, value: String): Boolean {
        return connection.run {
            sismember(key.value, value)
        }
    }

    override suspend fun hget(key: RedisKey, field: String): String? {
        return connection.run {
            hget(key.value, field)
        }
    }

    override suspend fun hgetall(key: RedisKey): Map<String, String>? {
        return connection.run {
            hgetall(key.value)
        }
    }

    override suspend fun hset(key: RedisKey, values: Map<String, String>) {
        connection.run {
            hset(key.value, values)
        }
    }

    override suspend fun hdel(key: RedisKey, field: String) {
        connection.run {
            hdel(key.value, field)
        }
    }

    override suspend fun expire(key: RedisKey, ttl: Duration) {
        connection.run {
            expire(key.value, ttl.inWholeSeconds)
        }
    }

    private suspend inline fun <reified R> Deferred<StatefulRedisConnection<String, String>>.run(
        crossinline action: RedisAsyncCommands<String, String>.() -> RedisFuture<R>
    ): R {
        return Metrics.with({ RedisCommand }, { e -> RedisCommandFailure(e) }) {
            await().async().action().asDeferred().await()
        }
    }
}
