package com.amplitude.deployment

import com.amplitude.RedisConfiguration
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.util.Redis
import com.amplitude.util.RedisConnection
import com.amplitude.util.RedisKey
import com.amplitude.util.json
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString

internal interface DeploymentStorage {
    suspend fun getDeployments(): Set<String>
    suspend fun putDeployment(deploymentKey: String)
    suspend fun removeDeploymentInternal(deploymentKey: String)
    suspend fun getFlag(deploymentKey: String, flagKey: String): EvaluationFlag?
    suspend fun getAllFlags(deploymentKey: String): Map<String, EvaluationFlag>
    suspend fun putFlag(deploymentKey: String, flag: EvaluationFlag)
    suspend fun putAllFlags(deploymentKey: String, flags: List<EvaluationFlag>)
    suspend fun removeFlag(deploymentKey: String, flagKey: String)
    suspend fun removeAllFlags(deploymentKey: String)
}

internal fun getDeploymentStorage(projectId: String, redisConfiguration: RedisConfiguration?): DeploymentStorage {
    val uri = redisConfiguration?.uri
    return if (uri == null) {
        InMemoryDeploymentStorage()
    } else {
        val redis = RedisConnection(uri)
        val readOnlyRedis = if (redisConfiguration.readOnlyUri != null) {
            RedisConnection(redisConfiguration.readOnlyUri)
        } else {
            redis
        }
        RedisDeploymentStorage(projectId, redisConfiguration.prefix, redis, readOnlyRedis)
    }
}

internal class InMemoryDeploymentStorage : DeploymentStorage {

    private val lock = Mutex()
    private val deploymentStorage = mutableMapOf<String, MutableMap<String, EvaluationFlag>?>()

    override suspend fun getDeployments(): Set<String> {
        return lock.withLock {
            deploymentStorage.keys.toSet()
        }
    }

    override suspend fun putDeployment(deploymentKey: String) {
        return lock.withLock {
            deploymentStorage.putIfAbsent(deploymentKey, null)
        }
    }

    override suspend fun removeDeploymentInternal(deploymentKey: String) {
        return lock.withLock {
            deploymentStorage.remove(deploymentKey)
        }
    }

    override suspend fun getFlag(deploymentKey: String, flagKey: String): EvaluationFlag? {
        return lock.withLock {
            deploymentStorage[deploymentKey]?.get(flagKey)
        }
    }

    override suspend fun getAllFlags(deploymentKey: String): Map<String, EvaluationFlag> {
        return lock.withLock {
            deploymentStorage[deploymentKey]?.toMap() ?: mapOf()
        }
    }

    override suspend fun putFlag(deploymentKey: String, flag: EvaluationFlag) {
        return lock.withLock {
            deploymentStorage[deploymentKey]?.put(flag.key, flag)
        }
    }

    override suspend fun putAllFlags(deploymentKey: String, flags: List<EvaluationFlag>) {
        return lock.withLock {
            deploymentStorage[deploymentKey]?.putAll(flags.associateBy { it.key })
        }
    }

    override suspend fun removeFlag(deploymentKey: String, flagKey: String) {
        return lock.withLock {
            deploymentStorage[deploymentKey]?.remove(flagKey)
        }
    }

    override suspend fun removeAllFlags(deploymentKey: String) {
        return lock.withLock {
            deploymentStorage[deploymentKey] = null
        }
    }
}

internal class RedisDeploymentStorage(
    private val prefix: String,
    private val projectId: String,
    private val redis: Redis,
    private val readOnlyRedis: Redis,
) : DeploymentStorage {

    override suspend fun getDeployments(): Set<String> {
        return redis.smembers(RedisKey.Deployments(prefix, projectId)) ?: emptySet()
    }

    override suspend fun putDeployment(deploymentKey: String) {
        redis.sadd(RedisKey.Deployments(prefix, projectId), setOf(deploymentKey))
    }

    override suspend fun removeDeploymentInternal(deploymentKey: String) {
        redis.srem(RedisKey.Deployments(prefix, projectId), deploymentKey)
    }

    override suspend fun getFlag(deploymentKey: String, flagKey: String): EvaluationFlag? {
        val flagJson = redis.hget(RedisKey.FlagConfigs(prefix, projectId, deploymentKey), flagKey) ?: return null
        return json.decodeFromString(flagJson)
    }

    // TODO Add in memory caching w/ invalidation
    override suspend fun getAllFlags(deploymentKey: String): Map<String, EvaluationFlag> {
        // High volume, use read only redis
        return readOnlyRedis.hgetall(RedisKey.FlagConfigs(prefix, projectId, deploymentKey))
            ?.mapValues { json.decodeFromString(it.value) } ?: mapOf()
    }

    override suspend fun putFlag(deploymentKey: String, flag: EvaluationFlag) {
        val flagJson = json.encodeToString(flag)
        redis.hset(RedisKey.FlagConfigs(prefix, projectId, deploymentKey), mapOf(flag.key to flagJson))
    }

    override suspend fun putAllFlags(deploymentKey: String, flags: List<EvaluationFlag>) {
        for (flag in flags) {
            putFlag(deploymentKey, flag)
        }
    }

    override suspend fun removeFlag(deploymentKey: String, flagKey: String) {
        redis.hdel(RedisKey.FlagConfigs(prefix, projectId, deploymentKey), flagKey)
    }

    override suspend fun removeAllFlags(deploymentKey: String) {
        val redisKey = RedisKey.FlagConfigs(prefix, projectId, deploymentKey)
        val flags = redis.hgetall(RedisKey.FlagConfigs(prefix, projectId, deploymentKey)) ?: return
        for (key in flags.keys) {
            redis.hdel(redisKey, key)
        }
    }
}
