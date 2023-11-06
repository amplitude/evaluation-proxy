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

    private suspend fun refresh() = lock.withLock {
        log.trace("refresh: start")
        // Get deployments from API and update the storage.
        val networkDeployments = projectApi.getDeployments().associateBy { it.key }
        val storageDeployments = deploymentStorage.getDeployments()
        // Determine added and removed deployments
        val addedDeployments = networkDeployments - storageDeployments.keys
        val removedDeployments = storageDeployments - networkDeployments.keys
        val startingDeployments = networkDeployments - deploymentRunners.keys
        val jobs = mutableListOf<Job>()
        for ((_, addedDeployment) in addedDeployments) {
            log.info("Adding deployment $addedDeployment")
            deploymentStorage.putDeployment(addedDeployment)
        }
        for ((_, deployment) in startingDeployments) {
            jobs += scope.launch { addDeploymentInternal(deployment.key) }
        }
        for ((_, removedDeployment) in removedDeployments) {
            log.info("Removing deployment $removedDeployment")
            deploymentStorage.removeAllFlags(removedDeployment.key)
            deploymentStorage.removeDeployment(removedDeployment.key)
            jobs += scope.launch { removeDeploymentInternal(removedDeployment.key) }
        }
        jobs.joinAll()
        // Keep cohorts which are targeted by all stored deployments.
        removeUnusedCohorts(networkDeployments.keys)
        log.debug(
            "Project refresh finished: addedDeployments={}, removedDeployments={}, startedDeployments={}",
            addedDeployments.keys,
            removedDeployments.keys,
            startingDeployments.keys
        )
        log.trace("refresh: end")
    }

    // Must be run within lock
    private suspend fun addDeploymentInternal(deploymentKey: String) {
        if (deploymentRunners.contains(deploymentKey)) {
            return
        }
        log.debug("Adding and starting deployment runner for $deploymentKey")
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
        log.debug("Removing and stopping deployment runner for $deploymentKey")
        deploymentRunners.remove(deploymentKey)?.stop()
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
