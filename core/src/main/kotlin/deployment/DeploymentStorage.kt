package com.amplitude.deployment

import com.amplitude.RedisConfiguration
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.util.RedisConnection
import com.amplitude.util.RedisKey
import com.amplitude.util.json
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString

interface DeploymentStorage {
    val deployments: Flow<Set<String>>
    suspend fun getDeployments(): Set<String>
    suspend fun putDeployment(deploymentKey: String)
    suspend fun removeDeployment(deploymentKey: String)
    suspend fun getFlagConfigs(deploymentKey: String): List<EvaluationFlag>?
    suspend fun putFlagConfigs(deploymentKey: String, flagConfigs: List<EvaluationFlag>)
    suspend fun removeFlagConfigs(deploymentKey: String)
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

    override val deployments = MutableSharedFlow<Set<String>>(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private val lock = Mutex()
    private val deploymentStorage = mutableMapOf<String, List<EvaluationFlag>?>()

    override suspend fun getDeployments(): Set<String> {
        return lock.withLock {
            deploymentStorage.keys.toSet()
        }
    }

    override suspend fun putDeployment(deploymentKey: String) {
        return lock.withLock {
            deploymentStorage[deploymentKey] = null
            deployments.emit(deploymentStorage.keys)
        }
    }

    override suspend fun removeDeployment(deploymentKey: String) {
        return lock.withLock {
            deploymentStorage.remove(deploymentKey)
            deployments.emit(deploymentStorage.keys)
        }
    }

    override suspend fun getFlagConfigs(deploymentKey: String): List<EvaluationFlag>? {
        return lock.withLock {
            deploymentStorage[deploymentKey]
        }
    }

    override suspend fun putFlagConfigs(deploymentKey: String, flagConfigs: List<EvaluationFlag>) {
        lock.withLock {
            deploymentStorage[deploymentKey] = flagConfigs
        }
    }

    override suspend fun removeFlagConfigs(deploymentKey: String) {
        lock.withLock {
            deploymentStorage.remove(deploymentKey)
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

    // TODO Could be optimized w/ pub sub
    override val deployments = MutableSharedFlow<Set<String>>(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private val mutex = Mutex()
    private val flagConfigCache: MutableList<EvaluationFlag> = mutableListOf()

    override suspend fun getDeployments(): Set<String> {
        return redis.smembers(RedisKey.Deployments(projectId)) ?: emptySet()
    }

    override suspend fun putDeployment(deploymentKey: String) {
        redis.sadd(RedisKey.Deployments(projectId), setOf(deploymentKey))
        deployments.emit(getDeployments())
    }

    override suspend fun removeDeployment(deploymentKey: String) {
        redis.srem(RedisKey.Deployments(projectId), deploymentKey)
        deployments.emit(getDeployments())
    }

    // TODO Add in memory caching w/ invalidation
    override suspend fun getFlagConfigs(deploymentKey: String): List<EvaluationFlag>? {
        // High volume, use read only redis
        val jsonEncodedFlags = readOnlyRedis.get(RedisKey.FlagConfigs(projectId, deploymentKey)) ?: return null
        return json.decodeFromString(jsonEncodedFlags)
    }

    override suspend fun putFlagConfigs(deploymentKey: String, flagConfigs: List<EvaluationFlag>) {
        // Optimization so repeat puts don't update the data to the same value in redis.
        val changed = mutex.withLock {
            if (flagConfigs != flagConfigCache) {
                flagConfigCache.clear()
                flagConfigCache += flagConfigs
                true
            } else {
                false
            }
        }
        if (changed) {
            val jsonEncodedFlags = json.encodeToString(flagConfigs)
            redis.set(RedisKey.FlagConfigs(projectId, deploymentKey), jsonEncodedFlags)
        }
    }

    override suspend fun removeFlagConfigs(deploymentKey: String) {
        redis.del(RedisKey.FlagConfigs(projectId, deploymentKey))
        mutex.withLock {
            flagConfigCache.clear()
        }
    }
}
