package com.amplitude.deployment

import com.amplitude.cohort.CohortLoader
import com.amplitude.util.getCohortIds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

class DeploymentRunner(
    @Volatile var configuration: DeploymentManagerConfiguration,
    private val deploymentKey: String,
    private val deploymentApi: DeploymentApi,
    private val deploymentStorage: DeploymentStorage,
    private val cohortLoader: CohortLoader,
) {

    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor)

    suspend fun start() {
        refreshFlagConfigsAndCohorts()
        // Periodic flag config and cohort refresher
        scope.launch {
            while (true) {
                delay(configuration.flagConfigPollerIntervalMillis)
                refreshFlagConfigsAndCohorts()
            }
        }
        // Periodic cohort refresher
        scope.launch {
            while (true) {
                delay(configuration.cohortPollerIntervalMillis)
                refreshCohorts()
            }
        }
    }

    suspend fun stop() {
        supervisor.cancelAndJoin()
    }

    private suspend fun refreshCohorts() {
        val flagConfigs = deploymentStorage.getFlagConfigs(deploymentKey) ?: return
        val cohortIds = flagConfigs.getCohortIds()
        cohortLoader.loadCohorts(cohortIds)
    }

    private suspend fun refreshFlagConfigsAndCohorts() {
        val storageFlagConfigs = deploymentStorage.getFlagConfigs(deploymentKey)
        val networkFlagConfigs = deploymentApi.getFlagConfigs(deploymentKey)
        if (storageFlagConfigs == networkFlagConfigs) {
            return
        }
        val cohortIds = networkFlagConfigs.getCohortIds()
        cohortLoader.loadCohorts(cohortIds)
        deploymentStorage.putFlagConfigs(deploymentKey, networkFlagConfigs)
    }
}
