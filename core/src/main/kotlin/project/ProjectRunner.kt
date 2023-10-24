package com.amplitude.project

import com.amplitude.Configuration
import com.amplitude.cohort.CohortApi
import com.amplitude.cohort.CohortLoader
import com.amplitude.cohort.CohortStorage
import com.amplitude.deployment.DeploymentApi
import com.amplitude.deployment.DeploymentRunner
import com.amplitude.deployment.DeploymentStorage
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.util.getCohortIds
import com.amplitude.util.logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class ProjectRunner(
    private val configuration: Configuration,
    private val projectApi: ProjectApi,
    private val deploymentApi: DeploymentApi,
    private val deploymentStorage: DeploymentStorage,
    cohortApi: CohortApi,
    private val cohortStorage: CohortStorage
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
        refresh()
        // Periodic deployment update and refresher
        scope.launch {
            while (true) {
                delay(configuration.deploymentSyncIntervalMillis)
                // Get deployments from API, update storage, and refresh
                val deployments = projectApi.getDeployments()
                for (deployment in deployments) {
                    deploymentStorage.putDeployment(deployment.key)
                }
                refresh()
            }
        }
    }

    suspend fun stop() {
        lock.withLock {
            for (deploymentRunner in deploymentRunners.values) {
                deploymentRunner.stop()
            }
        }
        supervisor.cancelAndJoin()
    }

    private suspend fun refresh() {
        log.debug("refresh: start")
        val deploymentKeys = deploymentStorage.getDeployments()
        lock.withLock {
            val jobs = mutableListOf<Job>()
            val runningDeployments = deploymentRunners.keys.toSet()
            val addedDeployments = deploymentKeys - runningDeployments
            val removedDeployments = runningDeployments - deploymentKeys
            addedDeployments.forEach { deployment ->
                jobs += scope.launch { addDeploymentInternal(deployment) }
            }
            removedDeployments.forEach { deployment ->
                jobs += scope.launch { removeDeploymentInternal(deployment) }
            }
            jobs.joinAll()
            // Keep cohorts which are targeted by all stored deployments.
            removeUnusedCohorts(deploymentKeys)
        }
        log.debug("refresh: end")
    }

    // Must be run within lock
    private suspend fun addDeploymentInternal(deploymentKey: String) {
        log.info("Adding deployment $deploymentKey")
        val deploymentRunner = DeploymentRunner(
            configuration,
            deploymentKey,
            deploymentApi,
            deploymentStorage,
            cohortLoader
        )
        deploymentRunner.start()
        deploymentRunners[deploymentKey] = deploymentRunner
    }

    // Must be run within lock
    private suspend fun removeDeploymentInternal(deploymentKey: String) {
        log.info("Removing deployment $deploymentKey")
        deploymentRunners.remove(deploymentKey)?.stop()
        deploymentStorage.removeAllFlags(deploymentKey)
        deploymentStorage.removeDeployment(deploymentKey)
    }

    private suspend fun removeUnusedCohorts(deploymentKeys: Set<String>) {
        val allFlagConfigs = mutableListOf<EvaluationFlag>()
        for (deploymentKey in deploymentKeys) {
            allFlagConfigs += deploymentStorage.getAllFlags(deploymentKey).values
        }
        val allTargetedCohortIds = allFlagConfigs.getCohortIds()
        val allStoredCohortDescriptions = cohortStorage.getCohortDescriptions().values
        for (cohortDescription in allStoredCohortDescriptions) {
            if (!allTargetedCohortIds.contains(cohortDescription.id)) {
                log.info("Removing unused cohort $cohortDescription")
                cohortStorage.removeCohort(cohortDescription)
            }
        }
    }
}
