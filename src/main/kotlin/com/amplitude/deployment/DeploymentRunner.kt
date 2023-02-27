package com.amplitude.deployment

import com.amplitude.cohort.CohortLoader
import com.amplitude.util.logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

data class DeploymentConfiguration(
    val flagConfigPollerIntervalMillis: Long = 10 * 1000,
    val cohortPollerIntervalMillis: Long = 60 * 1000,
    val maxCohortSize: Int = Int.MAX_VALUE,
)

class DeploymentRunner(
    @Volatile var configuration: DeploymentConfiguration,
    private val deploymentKey: String,
    private val deploymentApi: DeploymentApi,
    private val deploymentStorage: DeploymentStorage,
    private val cohortLoader: CohortLoader,
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
                delay(configuration.flagConfigPollerIntervalMillis)
                deploymentLoader.loadDeployment(deploymentKey)
            }
        }
        // Periodic cohort refresher
        scope.launch {
            while (true) {
                delay(configuration.cohortPollerIntervalMillis)
                val cohortIds = deploymentStorage.getCohortIds(deploymentKey)
                if (cohortIds != null) {
                    cohortLoader.loadCohorts(cohortIds)
                }
            }
        }
    }

    suspend fun stop() {
        log.debug("stop: - deploymentKey=$deploymentKey")
        supervisor.cancelAndJoin()
    }
}
