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

class ProjectRunner(
    private val configuration: Configuration,
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
        refresh(deploymentStorage.getDeployments())
        // Collect deployment updates from the storage
        scope.launch {
            deploymentStorage.deployments.collect { deployments ->
                refresh(deployments)
            }
        }
        // Periodic deployment refresher
        scope.launch {
            while (true) {
                delay(configuration.flagSyncIntervalMillis)
                refresh(deploymentStorage.getDeployments())
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

    private suspend fun refresh(deploymentKeys: Set<String>) {
        log.debug("refresh: start - deploymentKeys=$deploymentKeys")
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
        log.debug("refresh: end - deploymentKeys=$deploymentKeys")
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
        deploymentStorage.removeFlagConfigs(deploymentKey)
        deploymentStorage.removeDeployment(deploymentKey)
    }

    private suspend fun removeUnusedCohorts(deploymentKeys: Set<String>) {
        val allFlagConfigs = mutableListOf<EvaluationFlag>()
        for (deploymentKey in deploymentKeys) {
            allFlagConfigs += deploymentStorage.getFlagConfigs(deploymentKey) ?: continue
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
