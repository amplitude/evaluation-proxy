package com.amplitude.cohort

import com.amplitude.util.logger
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class CohortLoader(
    @Volatile var maxCohortSize: Int,
    private val cohortApi: CohortApi,
    private val cohortStorage: CohortStorage
) {

    companion object {
        val log by logger()
    }
    private val jobsMutex = Mutex()
    private val jobs = mutableMapOf<String, Job>()

    suspend fun loadCohorts(cohortIds: Set<String>, state: Set<String> = cohortIds) = coroutineScope {
        log.debug("loadCohorts: start - cohortIds=$cohortIds")

        // Get cohort descriptions from storage and network.
        val networkCohortDescriptions = cohortApi.getCohortDescriptions(state)

        // Filter cohorts received from network. Removes cohorts which are:
        //   1. Not requested for management by this function.
        //   2. Larger than the max size.
        //   3. Are equal to what has been downloaded already.
        val cohorts = networkCohortDescriptions.filter { networkCohortDescription ->
            val storageDescription = cohortStorage.getCohortDescription(networkCohortDescription.id)
            cohortIds.contains(networkCohortDescription.id) &&
                networkCohortDescription.size <= maxCohortSize &&
                networkCohortDescription.lastComputed > (storageDescription?.lastComputed ?: -1)
        }
        log.debug("loadCohorts: filtered network descriptions - $cohorts")

        // Download and store each cohort if a download job has not already been started.
        for (cohort in cohorts) {
            val job = jobsMutex.withLock {
                jobs.getOrPut(cohort.id) {
                    launch {
                        log.info("Downloading cohort. $cohort")
                        val cohortMembers = cohortApi.getCohortMembers(cohort)
                        cohortStorage.putCohort(cohort, cohortMembers)
                        jobsMutex.withLock { jobs.remove(cohort.id) }
                    }
                }
            }
            job.join()
        }
        log.debug("loadCohorts: end - cohortIds=$cohortIds")
    }
}
