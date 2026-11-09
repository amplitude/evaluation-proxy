package com.amplitude.deployment

import com.amplitude.RedisConfiguration
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.util.json
import com.amplitude.util.redis.Redis
import com.amplitude.util.redis.RedisKey
import com.amplitude.util.redis.createRedisConnections
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

internal interface DeploymentStorage {
    suspend fun getDeployment(deploymentKey: String): Deployment?

    suspend fun getDeployments(): Map<String, Deployment>

    suspend fun putDeployment(deployment: Deployment)

    suspend fun removeDeployment(deploymentKey: String)

    suspend fun getFlag(
        deploymentKey: String,
        flagKey: String,
    ): EvaluationFlag?

    suspend fun getAllFlags(deploymentKey: String): Map<String, EvaluationFlag>

    /**
     * Read all flags for a deployment for serving requests. Implementations may
     * serve this from a short-lived in-memory cache; background loaders that diff
     * against storage must use [getAllFlags] instead.
     */
    suspend fun getAllFlagsCached(deploymentKey: String): Map<String, EvaluationFlag> = getAllFlags(deploymentKey)

    suspend fun putFlag(
        deploymentKey: String,
        flag: EvaluationFlag,
    )

    suspend fun removeFlag(
        deploymentKey: String,
        flagKey: String,
    )

    suspend fun removeAllFlags(deploymentKey: String)
}

internal fun getDeploymentStorage(
    projectId: String,
    redisConfiguration: RedisConfiguration?,
    flagCacheTtlMillis: Long = 0,
): DeploymentStorage {
    val connections = createRedisConnections(redisConfiguration)
    return if (connections != null) {
        RedisDeploymentStorage(
            redisConfiguration!!.prefix,
            projectId,
            connections.primary,
            connections.readOnly,
            flagCacheTtlMillis,
        )
    } else {
        InMemoryDeploymentStorage()
    }
}

internal class InMemoryDeploymentStorage : DeploymentStorage {
    private val mutex = Mutex()

    private val deploymentStorage = mutableMapOf<String, Deployment>()
    private val flagStorage = mutableMapOf<String, MutableMap<String, EvaluationFlag>>()

    override suspend fun getDeployment(deploymentKey: String): Deployment? {
        return mutex.withLock {
            deploymentStorage[deploymentKey]
        }
    }

    override suspend fun getDeployments(): Map<String, Deployment> {
        return mutex.withLock {
            deploymentStorage.toMap()
        }
    }

    override suspend fun putDeployment(deployment: Deployment) {
        mutex.withLock {
            deploymentStorage[deployment.key] = deployment
        }
    }

    override suspend fun removeDeployment(deploymentKey: String) {
        return mutex.withLock {
            deploymentStorage.remove(deploymentKey)
            flagStorage.remove(deploymentKey)
        }
    }

    override suspend fun getFlag(
        deploymentKey: String,
        flagKey: String,
    ): EvaluationFlag? {
        return mutex.withLock {
            flagStorage[deploymentKey]?.get(flagKey)
        }
    }

    override suspend fun getAllFlags(deploymentKey: String): Map<String, EvaluationFlag> {
        return mutex.withLock {
            flagStorage[deploymentKey] ?: mapOf()
        }
    }

    override suspend fun putFlag(
        deploymentKey: String,
        flag: EvaluationFlag,
    ) {
        return mutex.withLock {
            flagStorage.getOrPut(deploymentKey) { mutableMapOf() }[flag.key] = flag
        }
    }

    override suspend fun removeFlag(
        deploymentKey: String,
        flagKey: String,
    ) {
        return mutex.withLock {
            flagStorage[deploymentKey]?.remove(flagKey)
        }
    }

    override suspend fun removeAllFlags(deploymentKey: String) {
        return mutex.withLock {
            flagStorage.remove(deploymentKey)
        }
    }
}

internal class RedisDeploymentStorage(
    private val prefix: String,
    private val projectId: String,
    private val redis: Redis,
    private val readOnlyRedis: Redis,
    /**
     * How long decoded flag configs are served from memory by [getAllFlagsCached]
     * before being re-read from Redis. 0 disables the cache.
     */
    private val flagCacheTtlMillis: Long = 0,
    private val nanoTime: () -> Long = System::nanoTime,
) : DeploymentStorage {
    private class CachedFlags(
        val flags: Map<String, EvaluationFlag>,
        val loadedAtNanos: Long,
    )

    private val flagCache = ConcurrentHashMap<String, CachedFlags>()

    // Bumped on every local write to a deployment's flags. A refill only stores its
    // result if the generation is unchanged since the refill started, so a load that
    // began before a write can't put pre-write flags back into the cache.
    private val flagCacheGenerations = ConcurrentHashMap<String, Long>()
    private val flagCacheTtlNanos = TimeUnit.MILLISECONDS.toNanos(flagCacheTtlMillis)

    private fun invalidate(deploymentKey: String) {
        // Bump before removing: a refill that checks the generation after this point
        // discards its result; one that stored before it is removed here.
        flagCacheGenerations.merge(deploymentKey, 1L, Long::plus)
        flagCache.remove(deploymentKey)
    }

    override suspend fun getDeployment(deploymentKey: String): Deployment? {
        val deploymentJson = redis.hget(RedisKey.Deployments(prefix, projectId), deploymentKey) ?: return null
        return json.decodeFromString(deploymentJson)
    }

    override suspend fun getDeployments(): Map<String, Deployment> {
        return redis.hgetall(RedisKey.Deployments(prefix, projectId))
            ?.mapValues { json.decodeFromString(it.value) } ?: mapOf()
    }

    override suspend fun putDeployment(deployment: Deployment) {
        val deploymentJson = json.encodeToString(deployment)
        redis.hset(RedisKey.Deployments(prefix, projectId), mapOf(deployment.key to deploymentJson))
    }

    override suspend fun removeDeployment(deploymentKey: String) {
        redis.hdel(RedisKey.Deployments(prefix, projectId), deploymentKey)
        removeAllFlags(deploymentKey)
        invalidate(deploymentKey)
    }

    override suspend fun getFlag(
        deploymentKey: String,
        flagKey: String,
    ): EvaluationFlag? {
        val flagJson = redis.hget(RedisKey.FlagConfigs(prefix, projectId, deploymentKey), flagKey) ?: return null
        return json.decodeFromString(flagJson)
    }

    override suspend fun getAllFlags(deploymentKey: String): Map<String, EvaluationFlag> {
        // High volume, use read only redis
        return readOnlyRedis.hgetall(RedisKey.FlagConfigs(prefix, projectId, deploymentKey))
            ?.mapValues { json.decodeFromString(it.value) } ?: mapOf()
    }

    /**
     * Serve flags from memory for up to [flagCacheTtlMillis] to avoid an HGETALL and
     * a full JSON decode of every flag on every request. Local writes invalidate the
     * entry immediately; writes made by other instances become visible within the TTL.
     *
     * Refills read the primary rather than the read replica: with the cache in front,
     * this is about one read per deployment per TTL, and it avoids caching a snapshot
     * that lags a write this instance just made.
     */
    override suspend fun getAllFlagsCached(deploymentKey: String): Map<String, EvaluationFlag> {
        if (flagCacheTtlNanos <= 0) {
            return getAllFlags(deploymentKey)
        }
        val now = nanoTime()
        val cached = flagCache[deploymentKey]
        if (cached != null && now - cached.loadedAtNanos < flagCacheTtlNanos) {
            return cached.flags
        }
        val generation = flagCacheGenerations[deploymentKey] ?: 0L
        val flags =
            redis.hgetall(RedisKey.FlagConfigs(prefix, projectId, deploymentKey))
                ?.mapValues { json.decodeFromString<EvaluationFlag>(it.value) } ?: mapOf()
        flagCache.compute(deploymentKey) { _, existing ->
            if ((flagCacheGenerations[deploymentKey] ?: 0L) == generation) CachedFlags(flags, now) else existing
        }
        return flags
    }

    override suspend fun putFlag(
        deploymentKey: String,
        flag: EvaluationFlag,
    ) {
        val flagJson = json.encodeToString(flag)
        redis.hset(RedisKey.FlagConfigs(prefix, projectId, deploymentKey), mapOf(flag.key to flagJson))
        invalidate(deploymentKey)
    }

    override suspend fun removeFlag(
        deploymentKey: String,
        flagKey: String,
    ) {
        redis.hdel(RedisKey.FlagConfigs(prefix, projectId, deploymentKey), flagKey)
        invalidate(deploymentKey)
    }

    override suspend fun removeAllFlags(deploymentKey: String) {
        val redisKey = RedisKey.FlagConfigs(prefix, projectId, deploymentKey)
        val flags = redis.hgetall(redisKey)
        if (flags != null) {
            for (flagKey in flags.keys) {
                redis.hdel(redisKey, flagKey)
            }
        }
        invalidate(deploymentKey)
    }
}
