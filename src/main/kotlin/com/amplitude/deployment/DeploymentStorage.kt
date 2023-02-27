package com.amplitude.deployment

import com.amplitude.experiment.evaluation.FlagConfig
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

data class DeploymentStorageConfiguration(
    val deploymentCacheExpiryMillis: Long = 1000
)

interface DeploymentStorage {
    var configuration: DeploymentStorageConfiguration
    val deployments: Flow<Set<String>>
    suspend fun getDeployments(): Set<String>
    suspend fun putDeployment(deploymentKey: String)
    suspend fun removeDeployment(deploymentKey: String)
    suspend fun getFlagConfigs(deploymentKey: String): List<FlagConfig>?
    suspend fun putFlagConfigs(deploymentKey: String, flagConfigs: List<FlagConfig>)
}

class InMemoryDeploymentStorage(
    override var configuration: DeploymentStorageConfiguration = DeploymentStorageConfiguration()
) : DeploymentStorage {

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
}
