package test

import com.amplitude.util.redis.Redis
import com.amplitude.util.redis.RedisKey
import kotlin.time.Duration

internal class InMemoryRedis : Redis {
    private val kv = mutableMapOf<String, String>()
    private val sets = mutableMapOf<String, MutableSet<String>>()
    private val hashes = mutableMapOf<String, MutableMap<String, String>>()

    override suspend fun get(key: RedisKey): String? {
        return kv[key.value]
    }

    override suspend fun set(
        key: RedisKey,
        value: String,
    ) {
        kv[key.value] = value
    }

    override suspend fun set(
        key: RedisKey,
        value: String,
        mode: String,
        seconds: Long,
        condition: String,
    ): String? {
        // Simple implementation for testing - ignores mode/seconds/condition
        val existing = kv[key.value]
        kv[key.value] = value
        return if (existing == null) "OK" else null
    }

    override suspend fun del(key: RedisKey) {
        kv.remove(key.value)
    }

    override suspend fun sadd(
        key: RedisKey,
        values: Set<String>,
    ) {
        sets.getOrPut(key.value) { mutableSetOf() }.addAll(values)
    }

    override suspend fun srem(
        key: RedisKey,
        values: Set<String>,
    ) {
        sets.getOrPut(key.value) { mutableSetOf() }.removeAll(values)
    }

    override suspend fun sscan(
        key: RedisKey,
        limit: Long,
    ): Set<String>? {
        return sets[key.value]?.toSet()
    }

    override suspend fun smembers(key: RedisKey): Set<String> {
        return sets[key.value] ?: emptySet()
    }

    override suspend fun sismember(
        key: RedisKey,
        value: String,
    ): Boolean {
        return sets[key.value]?.contains(value) ?: false
    }

    override suspend fun sdiff(
        key1: RedisKey,
        key2: RedisKey,
    ): Set<String>? {
        return sets[key1.value]?.subtract(sets[key2.value] ?: setOf())
    }

    override suspend fun sdiffstore(
        destKey: RedisKey,
        key1: RedisKey,
        key2: RedisKey,
    ): Long {
        val diff = sets[key1.value]?.subtract(sets[key2.value] ?: setOf()) ?: setOf()
        sets[destKey.value] = diff.toMutableSet()
        return diff.size.toLong()
    }

    override suspend fun sscanChunked(
        key: RedisKey,
        chunkSize: Int,
        processor: suspend (chunk: Set<String>) -> Unit,
    ) {
        val set = sets[key.value] ?: return
        set.chunked(chunkSize).forEach { chunk ->
            processor(chunk.toSet())
        }
    }

    override suspend fun hget(
        key: RedisKey,
        field: String,
    ): String? {
        return hashes.getOrPut(key.value) { mutableMapOf() }[field]
    }

    override suspend fun hgetall(key: RedisKey): Map<String, String>? {
        return hashes[key.value]?.toMap()
    }

    override suspend fun hset(
        key: RedisKey,
        values: Map<String, String>,
    ) {
        hashes.getOrPut(key.value) { mutableMapOf() }.putAll(values)
    }

    override suspend fun hdel(
        key: RedisKey,
        field: String,
    ) {
        hashes[key.value]?.remove(field)
        if (hashes[key.value]?.isEmpty() == true) {
            hashes.remove(key.value)
        }
    }

    override suspend fun expire(
        key: RedisKey,
        ttl: Duration,
    ) {
        // Do nothing.
    }

    override suspend fun saddPipeline(
        commands: List<Pair<RedisKey, Set<String>>>,
        batchSize: Int,
    ) {
        for (command in commands) {
            sadd(command.first, command.second)
        }
    }

    override suspend fun sremPipeline(
        commands: List<Pair<RedisKey, Set<String>>>,
        batchSize: Int,
    ) {
        for (command in commands) {
            srem(command.first, command.second)
        }
    }

    private val activeLocks = mutableMapOf<String, String>()

    override suspend fun acquireLock(
        key: RedisKey,
        ttlSeconds: Long,
    ): Boolean {
        val keyStr = key.value
        return if (kv.containsKey(keyStr)) {
            false // Lock already exists
        } else {
            val lockValue = "${System.currentTimeMillis()}-${Thread.currentThread().id}"
            kv[keyStr] = lockValue
            activeLocks[keyStr] = lockValue
            // Note: InMemory doesn't implement TTL expiration for simplicity
            true
        }
    }

    override suspend fun releaseLock(key: RedisKey): Boolean {
        val keyStr = key.value
        val expectedValue = activeLocks.remove(keyStr)
        
        return if (expectedValue != null) {
            val currentValue = kv[keyStr]
            if (currentValue == expectedValue) {
                kv.remove(keyStr)
                true
            } else {
                false
            }
        } else {
            false // No active lock to release
        }
    }
}
