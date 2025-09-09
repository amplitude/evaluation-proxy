package com.amplitude.util.redis

import com.amplitude.Metrics
import com.amplitude.RedisCommand
import com.amplitude.RedisCommandFailure
import com.amplitude.util.logger
import io.lettuce.core.ReadFrom
import io.lettuce.core.RedisFuture
import io.lettuce.core.RedisURI
import io.lettuce.core.ScanArgs
import io.lettuce.core.ScanCursor
import io.lettuce.core.TimeoutOptions
import io.lettuce.core.cluster.ClusterClientOptions
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands
import io.lettuce.core.codec.StringCodec
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors
import kotlin.time.Duration
import java.time.Duration as JavaDuration

internal class RedisClusterConnection(
    clusterUri: String,
    connectionTimeoutMillis: Long = 10000L,
    commandTimeoutMillis: Long = 5000L,
    readFrom: ReadFrom? = null,
) : Redis {
    private val pipelineConnection: Deferred<StatefulRedisClusterConnection<String, String>>
    private val pipelineContext = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private val connection: Deferred<StatefulRedisClusterConnection<String, String>>
    private val client: RedisClusterClient
    private val desiredReadFrom: ReadFrom? = readFrom

    // Track active lock values for safe release
    private val activeLocks = mutableMapOf<String, String>()

    companion object {
        val log by logger()
    }

    init {
        // Create RedisURI for the cluster (can be configuration endpoint or individual nodes)
        val uri =
            RedisURI.create(clusterUri).apply {
                timeout = JavaDuration.ofMillis(connectionTimeoutMillis)
            }
        client = RedisClusterClient.create(uri)
        val topologyRefreshOptions =
            ClusterTopologyRefreshOptions.builder()
                .enableAllAdaptiveRefreshTriggers()
                .refreshPeriod(JavaDuration.ofSeconds(30))
                .build()
        val clientOptions =
            ClusterClientOptions.builder()
                .topologyRefreshOptions(topologyRefreshOptions)
                .timeoutOptions(
                    TimeoutOptions.builder()
                        .fixedTimeout(JavaDuration.ofMillis(commandTimeoutMillis))
                        .build(),
                )
                .autoReconnect(true)
                .build()
        client.setOptions(clientOptions)
        client.partitions
        connection = client.connectAsync(StringCodec.UTF8).asDeferred()
        pipelineConnection = client.connectAsync(StringCodec.UTF8).asDeferred()
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

    override suspend fun set(
        key: RedisKey,
        value: String,
        mode: String,
        seconds: Long,
        condition: String,
    ): String? {
        return connection.run {
            // Use Lettuce's conditional SET with EX and NX
            set(key.value, value, io.lettuce.core.SetArgs().ex(seconds).nx())
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

    override suspend fun smembers(key: RedisKey): Set<String> {
        return connection.run { smembers(key.value) }
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

    override suspend fun sdiffstore(
        destKey: RedisKey,
        key1: RedisKey,
        key2: RedisKey,
    ): Long {
        return connection.run {
            sdiffstore(destKey.value, key1.value, key2.value)
        }
    }

    override suspend fun sscanChunked(
        key: RedisKey,
        chunkSize: Int,
        processor: suspend (chunk: Set<String>) -> Unit,
    ) {
        var cursor = ScanCursor.INITIAL
        do {
            cursor =
                connection.run {
                    sscan(key.value, cursor, ScanArgs().limit(chunkSize.toLong()))
                }
            if (cursor.values.isNotEmpty()) {
                processor(cursor.values.toSet())
            }
        } while (!cursor.isFinished)
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

    override suspend fun saddPipeline(
        commands: List<Pair<RedisKey, Set<String>>>,
        batchSize: Int,
    ) {
        for (i in 0 until commands.size step batchSize) {
            pipeline {
                val batch = commands.subList(i, minOf(i + batchSize, commands.size))
                val futures = mutableListOf<io.lettuce.core.RedisFuture<Long>>()
                batch.forEach { (key, values) ->
                    futures += sadd(key.value, *values.toTypedArray())
                }
                futures
            }
        }
    }

    override suspend fun sremPipeline(
        commands: List<Pair<RedisKey, Set<String>>>,
        batchSize: Int,
    ) {
        for (i in 0 until commands.size step batchSize) {
            pipeline {
                val batch = commands.subList(i, minOf(i + batchSize, commands.size))
                val futures = mutableListOf<io.lettuce.core.RedisFuture<Long>>()
                batch.forEach { (key, values) ->
                    futures += srem(key.value, *values.toTypedArray())
                }
                futures
            }
        }
    }

    override suspend fun acquireLock(
        key: RedisKey,
        ttlSeconds: Long,
    ): Boolean {
        // Generate unique lock value internally
        val lockValue = "${'$'}{System.currentTimeMillis()}-${'$'}{Thread.currentThread().id}-${'$'}{java.util.UUID.randomUUID()}"

        val result =
            connection.run {
                set(key.value, lockValue, io.lettuce.core.SetArgs().ex(ttlSeconds).nx())
            }

        val acquired = result == "OK"
        if (acquired) {
            synchronized(activeLocks) {
                activeLocks[key.value] = lockValue
            }
        }

        return acquired
    }

    override suspend fun releaseLock(key: RedisKey): Boolean {
        val lockValue =
            synchronized(activeLocks) {
                activeLocks[key.value]
            } ?: return false

        // Use Lua script for atomic compare-and-delete
        val script =
            """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            """.trimIndent()

        val result =
            connection.run {
                eval<Long>(script, io.lettuce.core.ScriptOutputType.INTEGER, arrayOf(key.value), lockValue)
            }

        val released = result == 1L
        if (released) {
            synchronized(activeLocks) {
                activeLocks.remove(key.value)
            }
        }

        return released
    }

    suspend fun pipeline(block: suspend RedisAdvancedClusterAsyncCommands<String, String>.() -> List<RedisFuture<*>>) {
        withContext(pipelineContext) {
            val conn = pipelineConnection.await()
            if (desiredReadFrom != null) {
                try {
                    conn.setReadFrom(desiredReadFrom)
                } catch (_: Throwable) {
                }
            }
            val commands = conn.async()
            commands.setAutoFlushCommands(false)
            val futures =
                try {
                    block.invoke(commands)
                } finally {
                    commands.flushCommands()
                }
            // Await completion of the batch after flushing, to bound in-flight replies
            futures.forEach { it.asDeferred().await() }
        }
    }

    private suspend inline fun <reified R> Deferred<StatefulRedisClusterConnection<String, String>>.run(
        crossinline action: RedisAdvancedClusterAsyncCommands<String, String>.() -> RedisFuture<R>,
    ): R {
        return Metrics.with({ RedisCommand }, { e -> RedisCommandFailure(e) }) {
            val conn = await()
            if (desiredReadFrom != null) {
                try {
                    conn.setReadFrom(desiredReadFrom)
                } catch (_: Throwable) {
                }
            }
            conn.async().action().asDeferred().await()
        }
    }
}
