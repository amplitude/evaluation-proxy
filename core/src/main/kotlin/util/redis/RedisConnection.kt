package com.amplitude.util.redis

import com.amplitude.Metrics
import com.amplitude.RedisCommand
import com.amplitude.RedisCommandFailure
import com.amplitude.util.logger
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

internal class RedisConnection(
    redisUri: String,
    connectionTimeoutMillis: Long = 10000L,
    commandTimeoutMillis: Long = 5000L,
) : Redis {
    private val pipelineConnection: Deferred<StatefulRedisConnection<String, String>>
    private val pipelineContext = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    private val connection: Deferred<StatefulRedisConnection<String, String>>
    private val client: RedisClient
    
    // Track active lock values for safe release
    private val activeLocks = mutableMapOf<String, String>()

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
                val futures = mutableListOf<RedisFuture<Long>>()
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
                val futures = mutableListOf<RedisFuture<Long>>()
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
        val lockValue = "${System.currentTimeMillis()}-${Thread.currentThread().id}-${java.util.UUID.randomUUID()}"
        
        val result = connection.run {
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
        val lockValue = synchronized(activeLocks) {
            activeLocks.remove(key.value)
        }
        
        return if (lockValue != null) {
            // Use Lua script for atomic compare-and-delete
            val luaScript = """
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    return redis.call("DEL", KEYS[1])
                else
                    return 0
                end
            """.trimIndent()
            
            val result = connection.run {
                eval<Long>(luaScript, io.lettuce.core.ScriptOutputType.INTEGER, arrayOf(key.value), lockValue)
            }
            
            val released = result == 1L
            if (!released) {
                log.warn("Failed to release lock for key ${key.value} - lock may have expired or been taken by another process")
            }
            released
        } else {
            log.warn("Attempted to release lock for key ${key.value} but no active lock found")
            false
        }
    }

    private suspend fun pipeline(
        block: RedisAsyncCommands<String, String>.() -> List<RedisFuture<*>>,
    ) {
        withContext(pipelineContext) {
            val connection = pipelineConnection.await()
            connection.setAutoFlushCommands(false)
            val futures = try {
                block.invoke(connection.async())
            } finally {
                // Always flush whatever we queued before awaiting results
                connection.flushCommands()
            }
            // Await completion of the batch to avoid unbounded in-flight replies
            futures.forEach { future ->
                future.asDeferred().await()
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
