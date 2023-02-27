package com.amplitude.cohort

import com.amplitude.util.logger
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface ICohortLoader {
    suspend fun loadCohorts(cohortIds: Set<String>)
}

class CohortLoader(
    @Volatile var maxCohortSize: Int,
    private val cohortApi: CohortApi,
    private val cohortStorage: CohortStorage,
) {

    companion object {
        val log by logger()
    }
    private val jobsMutex = Mutex()
    private val jobs = mutableMapOf<String, Job>()

    suspend fun loadCohorts(cohortIds: Set<String>) = coroutineScope {
        log.debug("loadCohorts: start - cohortIds=$cohortIds")

        // Get cohort descriptions from storage and network.
        val networkCohortDescriptions = cohortApi.getCohortDescriptions()

        // Filter cohorts received from network. Removes cohorts which are:
        //   1. Not request for management by this function.
        //   2. Larger than the max size.
        //   3. Are equal to what has been downloaded already.
        val cohorts = networkCohortDescriptions.filter { networkCohortDescription ->
            val storageDescription = cohortStorage.getCohortDescription(networkCohortDescription.id)
            cohortIds.contains(networkCohortDescription.id) &&
                networkCohortDescription.size <= maxCohortSize &&
                networkCohortDescription.lastComputed > (storageDescription?.lastComputed ?: -1)
        }

        // Download and store each cohort if a download job has not already been started.
        for (cohort in cohorts) {
            jobsMutex.withLock {
                jobs.getOrPut(cohort.id) {
                    launch {
                        val cohortMembers = cohortApi.getCohortMembers(cohort)
                        cohortStorage.putCohort(cohort, cohortMembers)
                    }
                }
            }.join()
        }
        log.debug("loadCohorts: end - cohortIds=$cohortIds")
    }
}
