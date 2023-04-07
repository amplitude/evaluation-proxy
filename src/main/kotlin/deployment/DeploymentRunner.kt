package com.amplitude.deployment

import com.amplitude.cohort.CohortLoader
import com.amplitude.util.logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import project.ProjectConfiguration

class DeploymentRunner(
    @Volatile var configuration: ProjectConfiguration,
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
        log.debug("start: - deploymentKey=$deploymentKey")
        deploymentLoader.loadDeployment(deploymentKey)
        log.debug("start: loaded deployment - deploymentKey=$deploymentKey")
        // Periodic flag config and cohort refresher
        scope.launch {
            while (true) {
                delay(configuration.syncIntervalMillis)
                deploymentLoader.loadDeployment(deploymentKey)
            }
        }
    }

    suspend fun stop() {
        log.debug("stop: - deploymentKey=$deploymentKey")
        supervisor.cancelAndJoin()
    }
}