package com.amplitude.deployment

import com.amplitude.experiment.evaluation.FlagConfig
import com.amplitude.experiment.evaluation.serialization.SerialFlagConfig
import com.amplitude.log
import com.amplitude.util.Redis
import com.amplitude.util.RedisKey
import com.amplitude.util.getCohortIds
import com.amplitude.util.json
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString

interface DeploymentStorage {
    val deployments: Flow<Set<String>>
    suspend fun getDeployments(): Set<String>
    suspend fun putDeployment(deploymentKey: String)
    suspend fun removeDeployment(deploymentKey: String)
    suspend fun getFlagConfigs(deploymentKey: String): List<FlagConfig>?
    suspend fun putFlagConfigs(deploymentKey: String, flagConfigs: List<FlagConfig>)
    suspend fun removeFlagConfigs(deploymentKey: String)
    suspend fun getCohortIds(deploymentKey: String): Set<String>?
}

class InMemoryDeploymentStorage : DeploymentStorage {

    override val deployments = MutableSharedFlow<Set<String>>(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private val lock = Mutex()
    private val deploymentStorage = mutableMapOf<String, List<FlagConfig>?>()

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

    override suspend fun getFlagConfigs(deploymentKey: String): List<FlagConfig>? {
        return lock.withLock {
            deploymentStorage[deploymentKey]
        }
    }

    override suspend fun putFlagConfigs(deploymentKey: String, flagConfigs: List<FlagConfig>) {
        lock.withLock {
            deploymentStorage[deploymentKey] = flagConfigs
        }
    }

    override suspend fun removeFlagConfigs(deploymentKey: String) {
        lock.withLock {
            deploymentStorage.remove(deploymentKey)
        }
    }

    override suspend fun getCohortIds(deploymentKey: String): Set<String>? {
        return lock.withLock {
            deploymentStorage[deploymentKey]?.getCohortIds()
        }
    }
}

class RedisDeploymentStorage(
    redisUrl: String,
    prefix: String
) : DeploymentStorage {

    private val redis = Redis(redisUrl, prefix)

    // TODO Could be optimized w/ pub sub
    override val deployments = MutableSharedFlow<Set<String>>(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private val mutex = Mutex()
    private var flagConfigCache: List<FlagConfig>? = null

    override suspend fun getDeployments(): Set<String> {
        return redis.smembers(RedisKey.Deployments) ?: emptySet()
    }

    override suspend fun putDeployment(deploymentKey: String) {
        redis.sadd(RedisKey.Deployments, deploymentKey)
        deployments.emit(getDeployments())
    }

    override suspend fun removeDeployment(deploymentKey: String) {
        redis.srem(RedisKey.Deployments, deploymentKey)
        deployments.emit(getDeployments())
    }

    override suspend fun getFlagConfigs(deploymentKey: String): List<FlagConfig>? {
        val jsonEncodedFlags = redis.get(RedisKey.FlagConfigs(deploymentKey)) ?: return null
        return json.decodeFromString<List<SerialFlagConfig>>(jsonEncodedFlags).map { it.convert() }
    }

    override suspend fun putFlagConfigs(deploymentKey: String, flagConfigs: List<FlagConfig>) {
        // Optimization so repeat puts don't update the data to the same value in redis.
        val changed = mutex.withLock {
            if (flagConfigs != flagConfigCache) {
                flagConfigCache = flagConfigs
                true
            } else {
                false
            }
        }
        if (changed) {
            val jsonEncodedFlags = json.encodeToString(flagConfigs.map { SerialFlagConfig(it) })
            redis.set(RedisKey.FlagConfigs(deploymentKey), jsonEncodedFlags)
        }
    }

    override suspend fun removeFlagConfigs(deploymentKey: String) {
        redis.del(RedisKey.FlagConfigs(deploymentKey))
    }

    override suspend fun getCohortIds(deploymentKey: String): Set<String>? {
        return getFlagConfigs(deploymentKey)?.getCohortIds()
    }
}
