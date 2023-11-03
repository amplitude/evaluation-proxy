package com.amplitude.deployment

import com.amplitude.FlagsFetch
import com.amplitude.FlagsFetchFailure
import com.amplitude.Metrics
import com.amplitude.cohort.CohortLoader
import com.amplitude.util.getCohortIds
import com.amplitude.util.logger
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class DeploymentLoader(
    private val deploymentApi: DeploymentApi,
    private val deploymentStorage: DeploymentStorage,
    private val cohortLoader: CohortLoader
) {

    companion object {
        val log by logger()
    }

    private val jobsMutex = Mutex()
    private val jobs = mutableMapOf<String, Job>()

    suspend fun loadDeployment(deploymentKey: String) = coroutineScope {
        log.trace("loadDeployment: - deploymentKey=$deploymentKey")
        jobsMutex.withLock {
            jobs.getOrPut(deploymentKey) {
                launch {
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
                        val cohortIds = flag.getCohortIds()
                        if (cohortIds.isNotEmpty()) {
                            launch {
                                cohortLoader.loadCohorts(cohortIds)
                                deploymentStorage.putFlag(deploymentKey, flag)
                            }
                        } else {
                            deploymentStorage.putFlag(deploymentKey, flag)
                        }
                    }
                    // Remove the job
                    jobsMutex.withLock { jobs.remove(deploymentKey) }
                }
            }
        }.join()
        log.trace("loadDeployment: end - deploymentKey=$deploymentKey")
    }
}
