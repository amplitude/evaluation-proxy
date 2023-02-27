package com.amplitude.cohort

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

data class CohortLoaderConfiguration(
    val maxCohortSize: Int = Int.MAX_VALUE,
)

class CohortLoader(
    @Volatile var configuration: CohortLoaderConfiguration,
    private val cohortApi: CohortApi,
    private val cohortStorage: CohortStorage,
) {

    private val scope = CoroutineScope(SupervisorJob())
    private val cohortDownloadJobsMutex = Mutex()
    private val cohortDownloadJobs = mutableMapOf<String, Job>()

    suspend fun loadCohorts(cohortIds: Set<String>) {
        // Get cohort descriptions from storage and network.
        // TODO handle api failure
        val networkCohortDescriptions = cohortApi.getCohortDescriptions()

        // Filter cohorts received from network. Removes cohorts which are:
        //   1. Not request for management by this function.
        //   2. Larger than the max size.
        //   3. Are equal to what has been downloaded already.
        val cohorts = networkCohortDescriptions.filter { networkCohortDescription ->
            // TODO handle storage failure
            val storageDescription = cohortStorage.getCohortDescription(networkCohortDescription.id)
            cohortIds.contains(networkCohortDescription.id) &&
                networkCohortDescription.size <= configuration.maxCohortSize &&
                networkCohortDescription.lastComputed > (storageDescription?.lastComputed ?: -1)
        }

        // Download and store each cohort if a download job has not already been started.
        for (cohort in cohorts) {
            cohortDownloadJobsMutex.withLock {
                cohortDownloadJobs.getOrPut(cohort.id) {
                    scope.launch {
                        // TODO handle api failure
                        val cohortMembers = cohortApi.getCohortMembers(cohort)
                        // TODO handle storage failure
                        cohortStorage.putCohort(cohort, cohortMembers)
                    }
                }
            }.join()
        }
    }
}
