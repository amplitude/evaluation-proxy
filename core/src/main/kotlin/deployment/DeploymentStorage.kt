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
    suspend fun getDeployment(deploymentKey: String): Deployment?
    suspend fun getDeployments(): Map<String, Deployment>
    suspend fun putDeployment(deployment: Deployment)
    suspend fun removeDeployment(deploymentKey: String)
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
        RedisDeploymentStorage(redisConfiguration.prefix, projectId, redis, readOnlyRedis)
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

    override suspend fun getFlag(deploymentKey: String, flagKey: String): EvaluationFlag? {
        return mutex.withLock {
            flagStorage[deploymentKey]?.get(flagKey)
        }
    }

    override suspend fun getAllFlags(deploymentKey: String): Map<String, EvaluationFlag> {
        return mutex.withLock {
            flagStorage[deploymentKey] ?: mapOf()
        }
    }

    override suspend fun putFlag(deploymentKey: String, flag: EvaluationFlag) {
        return mutex.withLock {
            flagStorage.getOrPut(deploymentKey) { mutableMapOf() }[flag.key] = flag
        }
    }

    override suspend fun putAllFlags(deploymentKey: String, flags: List<EvaluationFlag>) {
        return mutex.withLock {
            flagStorage.getOrPut(deploymentKey) { mutableMapOf() }.putAll(flags.associateBy { it.key })
        }
    }

    override suspend fun removeFlag(deploymentKey: String, flagKey: String) {
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
    private val readOnlyRedis: Redis
) : DeploymentStorage {
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
