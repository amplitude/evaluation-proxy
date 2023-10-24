package com.amplitude.deployment

import com.amplitude.RedisConfiguration
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.util.RedisConnection
import com.amplitude.util.RedisKey
import com.amplitude.util.json
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString

interface DeploymentStorage {
    suspend fun getDeployments(): Set<String>
    suspend fun putDeployment(deploymentKey: String)
    suspend fun removeDeployment(deploymentKey: String)
    suspend fun getFlag(deploymentKey: String, flagKey: String): EvaluationFlag?
    suspend fun getAllFlags(deploymentKey: String): Map<String, EvaluationFlag>
    suspend fun putFlag(deploymentKey: String, flag: EvaluationFlag)
    suspend fun putAllFlags(deploymentKey: String, flags: List<EvaluationFlag>)
    suspend fun removeFlag(deploymentKey: String, flagKey: String)
    suspend fun removeAllFlags(deploymentKey: String)
}

fun getDeploymentStorage(projectId: String, redisConfiguration: RedisConfiguration?): DeploymentStorage {
    val uri = redisConfiguration?.uri
    val readOnlyUri = redisConfiguration?.readOnlyUri ?: uri
    val prefix = redisConfiguration?.prefix
    return if (uri == null || readOnlyUri == null || prefix == null) {
        InMemoryDeploymentStorage()
    } else {
        RedisDeploymentStorage(uri, readOnlyUri, prefix, projectId)
    }
}

class InMemoryDeploymentStorage : DeploymentStorage {

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

    override suspend fun removeDeployment(deploymentKey: String) {
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

class RedisDeploymentStorage(
    uri: String,
    readOnlyUri: String,
    prefix: String,
    private val projectId: String
) : DeploymentStorage {

    private val redis = RedisConnection(uri, prefix)
    private val readOnlyRedis = RedisConnection(readOnlyUri, prefix)

    override suspend fun getDeployments(): Set<String> {
        return redis.smembers(RedisKey.Deployments(projectId)) ?: emptySet()
    }

    override suspend fun putDeployment(deploymentKey: String) {
        redis.sadd(RedisKey.Deployments(projectId), setOf(deploymentKey))
    }

    override suspend fun removeDeployment(deploymentKey: String) {
        redis.srem(RedisKey.Deployments(projectId), deploymentKey)
    }

    override suspend fun getFlag(deploymentKey: String, flagKey: String): EvaluationFlag? {
        val flagJson = redis.hget(RedisKey.FlagConfigs(projectId, deploymentKey), flagKey) ?: return null
        return json.decodeFromString(flagJson)
    }

    // TODO Add in memory caching w/ invalidation
    override suspend fun getAllFlags(deploymentKey: String): Map<String, EvaluationFlag> {
        // High volume, use read only redis
        return readOnlyRedis.hgetall(RedisKey.FlagConfigs(projectId, deploymentKey))
            ?.mapValues { json.decodeFromString(it.value) } ?: mapOf()
    }

    override suspend fun putFlag(deploymentKey: String, flag: EvaluationFlag) {
        val flagJson = json.encodeToString(flag)
        redis.hset(RedisKey.FlagConfigs(projectId, deploymentKey), mapOf(flag.key to flagJson))
    }

    override suspend fun putAllFlags(deploymentKey: String, flags: List<EvaluationFlag>) {
        for (flag in flags) {
            putFlag(deploymentKey, flag)
        }
    }

    override suspend fun removeFlag(deploymentKey: String, flagKey: String) {
        redis.hdel(RedisKey.FlagConfigs(projectId, deploymentKey), flagKey)
    }

    override suspend fun removeAllFlags(deploymentKey: String) {
        val redisKey = RedisKey.FlagConfigs(projectId, deploymentKey)
        val flags = redis.hgetall(RedisKey.FlagConfigs(projectId, deploymentKey)) ?: return
        for (key in flags.keys) {
            redis.hdel(redisKey, key)
        }
    }
}
