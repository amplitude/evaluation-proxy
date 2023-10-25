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

    suspend fun loadCohort(cohortId: String) = coroutineScope {
        log.trace("loadCohort: start - cohortId={}", cohortId)
        val networkCohort = cohortApi.getCohortDescription(cohortId)
        val storageCohort = cohortStorage.getCohortDescription(cohortId)
        val shouldDownloadCohort = networkCohort.size <= maxCohortSize &&
            networkCohort.lastComputed > (storageCohort?.lastComputed ?: -1)
        if (shouldDownloadCohort) {
            jobsMutex.withLock {
                jobs.getOrPut(cohortId) {
                    launch {
                        log.info("Downloading cohort. $networkCohort")
                        val cohortMembers = cohortApi.getCohortMembers(networkCohort)
                        cohortStorage.putCohort(networkCohort, cohortMembers)
                        jobsMutex.withLock { jobs.remove(cohortId) }
                    }
                }
            }.join()
        }
        log.trace("loadCohort: end - cohortId={}", cohortId)
    }
}
