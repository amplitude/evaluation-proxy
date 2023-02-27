package com.amplitude.deployment

import com.amplitude.cohort.CohortLoader
import com.amplitude.util.getCohortIds
import com.amplitude.util.logger
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class DeploymentLoader(
    private val deploymentApi: DeploymentApi,
    private val deploymentStorage: DeploymentStorage,
    private val cohortLoader: CohortLoader,
) {

    companion object {
        val log by logger()
    }

    private val jobsMutex = Mutex()
    private val jobs = mutableMapOf<String, Job>()

    suspend fun loadDeployment(deploymentKey: String) = coroutineScope {
        log.debug("loadDeployment: start - deploymentKey=$deploymentKey")
        jobsMutex.withLock {
            jobs.getOrPut(deploymentKey) {
                launch {
                    val storageFlagConfigs = deploymentStorage.getFlagConfigs(deploymentKey)
                    val networkFlagConfigs = deploymentApi.getFlagConfigs(deploymentKey)
                    if (storageFlagConfigs != networkFlagConfigs) {
                        log.info("loadDeployment: network flags changed - deploymentKey=$deploymentKey")
                        val cohortIds = networkFlagConfigs.getCohortIds()
                        cohortLoader.loadCohorts(cohortIds)
                        deploymentStorage.putFlagConfigs(deploymentKey, networkFlagConfigs)
                    }
                }
            }
        }.join()
        log.debug("loadDeployment: end - deploymentKey=$deploymentKey")
    }
}
