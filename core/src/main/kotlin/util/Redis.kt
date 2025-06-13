package com.amplitude.util

import com.amplitude.Metrics
import com.amplitude.RedisCommand
import com.amplitude.RedisCommandFailure
import io.lettuce.core.ClientOptions
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.RedisURI
import io.lettuce.core.ScanArgs
import io.lettuce.core.ScanCursor
import io.lettuce.core.SocketOptions
import io.lettuce.core.TimeoutOptions
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.StringCodec
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors
import kotlin.time.Duration
import java.time.Duration as JavaDuration

private const val STORAGE_PROTOCOL_VERSION = "v4"

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

    data class UserCohortMemberships(
        val prefix: String,
        val projectId: String,
        val groupType: String,
        val groupName: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:memberships:$groupType:$groupName")
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
        values: Set<String>,
    )

    suspend fun sscan(
        key: RedisKey,
        limit: Long,
    ): Set<String>?

    suspend fun sismember(
        key: RedisKey,
        value: String,
    ): Boolean

    suspend fun sdiff(
        key1: RedisKey,
        key2: RedisKey,
    ): Set<String>?

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

    suspend fun saddPipeline(commands: List<Pair<RedisKey, Set<String>>>, batchSize: Int)
    suspend fun sremPipeline(commands: List<Pair<RedisKey, Set<String>>>, batchSize: Int)
}

internal class RedisConnection(
    redisUri: String,
    connectionTimeoutMillis: Long = 10000L,
    commandTimeoutMillis: Long = 5000L,
) : Redis {
    private val pipelineConnection: Deferred<StatefulRedisConnection<String, String>>
    private val pipelineContext = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private val connection: Deferred<StatefulRedisConnection<String, String>>
    private val client: RedisClient

    companion object {
        val log by logger()
    }

    init {
        // Configure Redis URI with timeout
        val uri =
            RedisURI.create(redisUri).apply {
                timeout = JavaDuration.ofMillis(connectionTimeoutMillis)
            }

        // Configure client options with timeouts
        val clientOptions =
            ClientOptions.builder()
                .socketOptions(
                    SocketOptions.builder()
                        .connectTimeout(JavaDuration.ofMillis(connectionTimeoutMillis))
                        .build(),
                )
                .timeoutOptions(
                    TimeoutOptions.builder()
                        .fixedTimeout(JavaDuration.ofMillis(commandTimeoutMillis))
                        .build(),
                )
                .build()

        client =
            RedisClient.create(uri).apply {
                setOptions(clientOptions)
            }

        connection = client.connectAsync(StringCodec.UTF8, uri).asDeferred()
        pipelineConnection = client.connectAsync(StringCodec.UTF8, uri).asDeferred()
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
        values: Set<String>,
    ) {
        connection.run {
            srem(key.value, *values.toTypedArray())
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

    override suspend fun sdiff(
        key1: RedisKey,
        key2: RedisKey,
    ): Set<String>? {
        return connection.run {
            sdiff(key1.value, key2.value)
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

    override suspend fun saddPipeline(commands: List<Pair<RedisKey, Set<String>>>, batchSize: Int) {
        for (i in 0 until commands.size step batchSize) {
            pipeline {
                val batch = commands.subList(i, minOf(i + batchSize, commands.size))
                batch.forEach { (key, values) ->
                    sadd(key.value, *values.toTypedArray())
                }
            }
        }
    }

    override suspend fun sremPipeline(
        commands: List<Pair<RedisKey, Set<String>>>,
        batchSize: Int
    ) {
        for (i in 0 until commands.size step batchSize) {
            pipeline {
                val batch = commands.subList(i, minOf(i + batchSize, commands.size))
                batch.forEach { (key, values) ->
                    srem(key.value, *values.toTypedArray())
                }
            }
        }
    }

    private suspend fun pipeline(block: RedisAsyncCommands<String, String>.() -> Unit) {
        withContext(pipelineContext) {
            val commands = pipelineConnection.await()
            commands.setAutoFlushCommands(false)
            try {
                block.invoke(commands.async())
            } finally {
                commands.flushCommands()
            }
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
