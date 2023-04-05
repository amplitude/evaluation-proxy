package com.amplitude.project

import com.amplitude.cohort.CohortApi
import com.amplitude.cohort.CohortLoader
import com.amplitude.cohort.CohortStorage
import com.amplitude.deployment.DeploymentApi
import com.amplitude.deployment.DeploymentRunner
import com.amplitude.deployment.DeploymentStorage
import com.amplitude.experiment.evaluation.FlagConfig
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
import project.ProjectConfiguration

class ProjectManager(
    @Volatile var configuration: ProjectConfiguration,
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
        log.debug("start")
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
                delay(configuration.syncIntervalMillis)
                val deployments = deploymentStorage.getDeployments()
                refresh(deployments)
            }
        }
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
            val currentDeployments = deploymentRunners.keys
            val addedDeployments = deploymentKeys - currentDeployments
            val removedDeployments = currentDeployments - deploymentKeys
            addedDeployments.forEach { deployment ->
                jobs += scope.launch { addDeployment(deployment) }
            }
            removedDeployments.forEach { deployment ->
                jobs += scope.launch { removeDeployment(deployment) }
            }
            jobs.joinAll()
            removeUnusedCohorts(deploymentKeys)
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
            cohortLoader
        )
        deploymentRunner.start()
        deploymentRunners[deploymentKey] = deploymentRunner
        log.debug("addDeployment: end - deploymentKey=$deploymentKey")
    }

    private suspend fun removeDeployment(deploymentKey: String) {
        log.debug("removeDeployment: start - deploymentKey=$deploymentKey")
        deploymentRunners.remove(deploymentKey)?.stop()
        deploymentStorage.removeFlagConfigs(deploymentKey)
        log.debug("removeDeployment: end - deploymentKey=$deploymentKey")
    }

    private suspend fun removeUnusedCohorts(deploymentKeys: Set<String>) {
        val allFlagConfigs = mutableListOf<FlagConfig>()
        for (deploymentKey in deploymentKeys) {
            allFlagConfigs += deploymentStorage.getFlagConfigs(deploymentKey) ?: continue
        }
        val allTargetedCohortIds = allFlagConfigs.getCohortIds()
        val allStoredCohortDescriptions = cohortStorage.getCohortDescriptions().values
        for (cohortDescription in allStoredCohortDescriptions) {
            if (!allTargetedCohortIds.contains(cohortDescription.id)) {
                log.info("Removing unused cohort: $cohortDescription")
                cohortStorage.removeCohort(cohortDescription)
            }
        }
    }
}
