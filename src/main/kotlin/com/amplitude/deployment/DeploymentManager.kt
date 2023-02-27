package com.amplitude.deployment

import com.amplitude.cohort.CohortLoader
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

data class DeploymentManagerConfiguration(
    val flagConfigPollerIntervalMillis: Long = 10 * 1000,
    val cohortPollerIntervalMillis: Long = 60 * 1000,
)

class DeploymentManager(
    @Volatile var configuration: DeploymentManagerConfiguration,
    private val deploymentApi: DeploymentApi,
    private val deploymentStorage: DeploymentStorage,
    private val cohortLoader: CohortLoader,
) {

    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor)
    private val lock = Mutex()
    private val deploymentRunners = mutableMapOf<String, DeploymentRunner>()

    suspend fun start() {
        // Collect deployment updates from the storage
        scope.launch {
            deploymentStorage.deployments.collect { deployments ->
                refresh(deployments)
            }
        }
    }

    suspend fun stop() {
        val deploymentManagers = lock.withLock { deploymentRunners.values }
        for (deploymentManager in deploymentManagers) {
            deploymentManager.stop()
        }
    }

    private suspend fun refresh(deploymentKeys: Set<String>) {
        lock.withLock {
            val jobs = mutableListOf<Job>()
            (deploymentKeys - deploymentRunners.keys).forEach { deployment ->
                jobs += scope.launch { addDeployment(deployment) }
            }
            (deploymentRunners.keys - deploymentKeys).forEach { deployment ->
                jobs += scope.launch { removeDeployment(deployment) }
            }
            jobs.joinAll()
        }
    }

    private suspend fun addDeployment(deploymentKey: String) {
        val deploymentRunner = DeploymentRunner(
            deploymentKey = deploymentKey,
            configuration = configuration,
            deploymentApi = deploymentApi,
            deploymentStorage = deploymentStorage,
            cohortLoader = cohortLoader,
        )
        deploymentRunner.start()
        deploymentRunners[deploymentKey] = deploymentRunner
    }
    private suspend fun removeDeployment(deploymentKey: String) {
        deploymentRunners.remove(deploymentKey)?.stop()
    }
}
