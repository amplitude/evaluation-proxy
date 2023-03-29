package com.amplitude.deployment

import com.amplitude.cohort.CohortApi
import com.amplitude.cohort.CohortLoader
import com.amplitude.cohort.CohortStorage
import com.amplitude.util.logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class DeploymentManager(
    @Volatile var configuration: DeploymentConfiguration,
    private val deploymentApi: DeploymentApi,
    private val deploymentStorage: DeploymentStorage,
    cohortApi: CohortApi,
    cohortStorage: CohortStorage,
) {

    companion object {
        val log by logger()
    }

    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor)

    private val lock = Mutex()
    private val deploymentRunners = mutableMapOf<String, DeploymentRunner>()
    private val cohortLoader = CohortLoader(configuration.maxCohortSize, cohortApi, cohortStorage)

    suspend fun start() {
        log.debug("start")
        // Collect deployment updates from the storage
        scope.launch {
            deploymentStorage.deployments.collect { deployments ->
                refresh(deployments)
            }
        }
        refresh(deploymentStorage.getDeployments())
    }

    suspend fun stop() {
        log.debug("stop")
        lock.withLock {
            for (deploymentRunner in deploymentRunners.values) {
                deploymentRunner.stop()
            }
        }
        supervisor.cancelAndJoin()
    }

    private suspend fun refresh(deploymentKeys: Set<String>) {
        log.debug("refresh: start - deploymentKeys=$deploymentKeys")
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
        log.debug("refresh: end - deploymentKeys=$deploymentKeys")
    }

    private suspend fun addDeployment(deploymentKey: String) {
        log.debug("addDeployment: start - deploymentKey=$deploymentKey")
        val deploymentRunner = DeploymentRunner(
            configuration,
            deploymentKey,
            deploymentApi,
            deploymentStorage,
            cohortLoader,
        )
        deploymentRunner.start()
        deploymentRunners[deploymentKey] = deploymentRunner
        log.debug("addDeployment: end - deploymentKey=$deploymentKey")
    }
    private suspend fun removeDeployment(deploymentKey: String) {
        log.debug("removeDeployment: start - deploymentKey=$deploymentKey")
        deploymentRunners.remove(deploymentKey)?.stop()
        log.debug("removeDeployment: end - deploymentKey=$deploymentKey")
    }
}
