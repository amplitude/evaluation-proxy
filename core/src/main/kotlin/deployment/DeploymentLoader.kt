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
                    val networkFlags = deploymentApi.getFlagConfigs(deploymentKey)
                    for (flag in networkFlags) {
                        val cohortIds = flag.getCohortIds()
                        if (cohortIds.isNotEmpty()) {
                            launch {
                                for (cohortId in cohortIds) {
                                    cohortLoader.loadCohort(cohortId)
                                }
                                deploymentStorage.putFlag(deploymentKey, flag)
                            }
                        } else {
                            deploymentStorage.putFlag(deploymentKey, flag)
                        }
                    }
                }
            }
        }.join()
        log.trace("loadDeployment: end - deploymentKey=$deploymentKey")
    }
}
