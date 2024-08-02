package test

import com.amplitude.util.Redis
import com.amplitude.util.RedisKey
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
        value: String,
    ) {
        sets.getOrPut(key.value) { mutableSetOf() }.remove(value)
    }

    override suspend fun smembers(key: RedisKey): Set<String>? {
        return sets[key.value]?.toSet()
    }

    override suspend fun sismember(
        key: RedisKey,
        value: String,
    ): Boolean {
        return sets[key.value]?.contains(value) ?: false
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
}
