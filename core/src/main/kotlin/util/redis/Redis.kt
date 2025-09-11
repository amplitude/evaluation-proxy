package com.amplitude.util.redis
import kotlin.time.Duration

internal interface Redis {
    suspend fun get(key: RedisKey): String?

    suspend fun set(
        key: RedisKey,
        value: String,
    )

    suspend fun set(
        key: RedisKey,
        value: String,
        mode: String,
        seconds: Long,
        condition: String,
    ): String?

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

    suspend fun smembers(key: RedisKey): Set<String>

    suspend fun sismember(
        key: RedisKey,
        value: String,
    ): Boolean

    suspend fun sdiff(
        key1: RedisKey,
        key2: RedisKey,
    ): Set<String>?

    suspend fun sdiffstore(
        destKey: RedisKey,
        key1: RedisKey,
        key2: RedisKey,
    ): Long

    suspend fun sscanChunked(
        key: RedisKey,
        chunkSize: Int = 10000,
        processor: suspend (chunk: Set<String>) -> Unit,
    )

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

    suspend fun saddPipeline(
        commands: List<Pair<RedisKey, Set<String>>>,
        batchSize: Int,
    )

    suspend fun sremPipeline(
        commands: List<Pair<RedisKey, Set<String>>>,
        batchSize: Int,
    )

    /**
     * Acquire a distributed lock with automatic expiration.
     * Returns true if lock was acquired, false if lock is already held.
     * Lock value is generated internally for safe release.
     */
    suspend fun acquireLock(
        key: RedisKey,
        ttlSeconds: Long,
    ): Boolean

    /**
     * Release a distributed lock.
     * Only releases locks that were acquired by this Redis instance.
     */
    suspend fun releaseLock(key: RedisKey): Boolean
}
