package com.amplitude.deployment

import com.amplitude.FlagsFetch
import com.amplitude.FlagsFetchFailure
import com.amplitude.Metrics
import com.amplitude.cohort.CohortLoader
import com.amplitude.util.Loader
import com.amplitude.util.getAllCohortIds
import com.amplitude.util.logger
import kotlinx.coroutines.launch

internal class DeploymentLoader(
    private val deploymentApi: DeploymentApi,
    private val deploymentStorage: DeploymentStorage,
    private val cohortLoader: CohortLoader
) {

    companion object {
        val log by logger()
    }

    private val loader = Loader()

    suspend fun loadDeployment(deploymentKey: String) {
        log.trace("loadDeployment: - deploymentKey=$deploymentKey")
        loader.load(deploymentKey) {
            val networkFlags = Metrics.with({ FlagsFetch }, { e -> FlagsFetchFailure(e) }) {
                deploymentApi.getFlagConfigs(deploymentKey)
            }
            // Remove flags that are no longer deployed.
            val networkFlagKeys = networkFlags.map { it.key }.toSet()
            val storageFlagKeys = deploymentStorage.getAllFlags(deploymentKey).map { it.key }.toSet()
            for (flagToRemove in storageFlagKeys - networkFlagKeys) {
                log.debug("Removing flag: $flagToRemove")
                deploymentStorage.removeFlag(deploymentKey, flagToRemove)
            }
            // Load cohorts for each flag independently then put the
            // flag into storage.
            for (flag in networkFlags) {
                val cohortIds = flag.getAllCohortIds()
                if (cohortIds.isNotEmpty()) {
                    launch {
                        cohortLoader.loadCohorts(cohortIds)
                        deploymentStorage.putFlag(deploymentKey, flag)
                    }
                } else {
                    deploymentStorage.putFlag(deploymentKey, flag)
                }
            }
        }
        log.trace("loadDeployment: end - deploymentKey=$deploymentKey")
    }
}
