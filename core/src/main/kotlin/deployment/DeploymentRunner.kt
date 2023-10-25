package com.amplitude.deployment

import com.amplitude.Configuration
import com.amplitude.cohort.CohortLoader
import com.amplitude.util.getCohortIds
import com.amplitude.util.logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

class DeploymentRunner(
    @Volatile var configuration: Configuration,
    private val deploymentKey: String,
    private val deploymentApi: DeploymentApi,
    private val deploymentStorage: DeploymentStorage,
    private val cohortLoader: CohortLoader
) {

    companion object {
        val log by logger()
    }

    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor)
    private val deploymentLoader = DeploymentLoader(deploymentApi, deploymentStorage, cohortLoader)

    suspend fun start() {
        log.trace("start: - deploymentKey=$deploymentKey")
        deploymentLoader.loadDeployment(deploymentKey)
        // Periodic flag config loader
        scope.launch {
            while (true) {
                delay(configuration.flagSyncIntervalMillis)
                deploymentLoader.loadDeployment(deploymentKey)
            }
        }
        // Periodic cohort refresher
        scope.launch {
            while (true) {
                delay(configuration.cohortSyncIntervalMillis)
                val cohortIds = deploymentStorage.getAllFlags(deploymentKey).values.getCohortIds()
                cohortLoader.loadCohorts(cohortIds)
            }
        }
    }

    suspend fun stop() {
        log.debug("stop: - deploymentKey=$deploymentKey")
        supervisor.cancelAndJoin()
    }
}
