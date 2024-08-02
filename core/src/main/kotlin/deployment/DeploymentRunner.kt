package com.amplitude.deployment

import com.amplitude.Configuration
import com.amplitude.cohort.CohortLoader
import com.amplitude.util.getAllCohortIds
import com.amplitude.util.logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

internal class DeploymentRunner(
    private val configuration: Configuration,
    private val deploymentKey: String,
    private val cohortLoader: CohortLoader,
    private val deploymentStorage: DeploymentStorage,
    private val deploymentLoader: DeploymentLoader,
) {
    companion object {
        val log by logger()
    }

    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor)

    suspend fun start() {
        log.trace("start: - deploymentKey=$deploymentKey")
        val job =
            scope.launch {
                try {
                    deploymentLoader.loadDeployment(deploymentKey)
                } catch (t: Throwable) {
                    // Catch failure and continue to run pollers. Assume deployment
                    // load will eventually succeed.
                    log.error("Load failed for deployment $deploymentKey", t)
                }
            }
        // Periodic flag config loader
        scope.launch {
            while (true) {
                delay(configuration.flagSyncIntervalMillis)
                try {
                    deploymentLoader.loadDeployment(deploymentKey)
                } catch (t: Throwable) {
                    log.error("Periodic deployment load failed for deployment $deploymentKey", t)
                }
            }
        }
        // Periodic cohort refresher
        scope.launch {
            while (true) {
                delay(configuration.cohortSyncIntervalMillis)
                try {
                    val cohortIds = deploymentStorage.getAllFlags(deploymentKey).values.getAllCohortIds()
                    cohortLoader.loadCohorts(cohortIds)
                } catch (t: Throwable) {
                    log.error("Periodic cohort load failed for deployment $deploymentKey", t)
                }
            }
        }
        job.join()
    }

    suspend fun stop() {
        log.debug("stop: - deploymentKey=$deploymentKey")
        supervisor.cancelAndJoin()
    }
}
