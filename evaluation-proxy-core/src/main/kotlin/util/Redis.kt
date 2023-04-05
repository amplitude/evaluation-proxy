package com.amplitude.util

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

const val STORAGE_PROTOCOL_VERSION = "v1"

sealed class RedisKey(val value: String) {

    object Deployments : RedisKey("$STORAGE_PROTOCOL_VERSION:deployments")

    data class FlagConfigs(
        val deploymentKey: String
    ) : RedisKey("$STORAGE_PROTOCOL_VERSION:deployments:$deploymentKey:flags")

    object CohortDescriptions : RedisKey("$STORAGE_PROTOCOL_VERSION:cohorts")

    data class CohortMembers(
        val cohortDescription: CohortDescription
    ) : RedisKey("$STORAGE_PROTOCOL_VERSION:cohorts:${cohortDescription.id}:users:${cohortDescription.lastComputed}")
}

// interface Redis {
//     suspend fun get(key: RedisKey): String?
//     suspend fun set(key: RedisKey, value: String)
//     suspend fun del(key: RedisKey)
//     suspend fun sadd(key: RedisKey, value: String)
//     suspend fun sadd(key: RedisKey, values: Set<String>)
//     suspend fun srem(key: RedisKey, value: String)
//     suspend fun smembers(key: RedisKey): Set<String>?
//     suspend fun sismember(key: RedisKey, value: String): Boolean
//     suspend fun hget(key: RedisKey, field: String): String?
//     suspend fun hvals(key: RedisKey): List<String>?
//     suspend fun hset(key: RedisKey, values: Map<String, String>)
//     suspend fun multi(action: Redis.() -> Unit)
// }

class Redis(redisUrl: String, private val prefix: String) {

    private val connection: Deferred<StatefulRedisConnection<String, String>>
    private val client: RedisClient = RedisClient.create(redisUrl)

    init {
        connection = client.connectAsync(StringCodec.UTF8, RedisURI.create(redisUrl)).asDeferred()
    }

    suspend fun get(key: RedisKey): String? {
        return connection.run {
            get(key.getPrefixedKey())
        }
    }

    suspend fun set(key: RedisKey, value: String) {
        connection.run {
            set(key.getPrefixedKey(), value)
        }
    }

    suspend fun del(key: RedisKey) {
        connection.run {
            del(key.getPrefixedKey())
        }
    }

    suspend fun sadd(key: RedisKey, value: String) {
        connection.run {
            sadd(key.getPrefixedKey(), value)
        }
    }

    suspend fun sadd(key: RedisKey, values: Set<String>) {
        connection.run {
            sadd(key.getPrefixedKey(), *values.toTypedArray())
        }
    }

    suspend fun srem(key: RedisKey, value: String) {
        connection.run {
            srem(key.getPrefixedKey(), value)
        }
    }

    suspend fun smembers(key: RedisKey): Set<String>? {
        return connection.run {
            smembers(key.getPrefixedKey())
        }
    }

    suspend fun sismember(key: RedisKey, value: String): Boolean {
        return connection.run {
            sismember(key.getPrefixedKey(), value)
        }
    }

    suspend fun hget(key: RedisKey, field: String): String? {
        return connection.run {
            hget(key.getPrefixedKey(), field)
        }
    }

    suspend fun hgetall(key: RedisKey): Map<String, String>? {
        return connection.run {
            hgetall(key.getPrefixedKey())
        }
    }

    suspend fun hset(key: RedisKey, values: Map<String, String>) {
        connection.run {
            hset(key.getPrefixedKey(), values)
        }
    }

    suspend fun hdel(key: RedisKey, field: String) {
        connection.run {
            hdel(key.getPrefixedKey(), field)
        }
    }

    suspend fun expire(key: RedisKey, ttl: Duration) {
        connection.run {
            expire(key.getPrefixedKey(), ttl.inWholeSeconds)
        }
    }

    private suspend inline fun <reified R> Deferred<StatefulRedisConnection<String, String>>.run(
        action: RedisAsyncCommands<String, String>.() -> RedisFuture<R>
    ): R {
        return await().async().action().asDeferred().await()
    }

    private fun RedisKey.getPrefixedKey(): String {
        return "$prefix:${this.value}"
    }
}
