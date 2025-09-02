package com.amplitude.cohort

import com.amplitude.CohortDownload
import com.amplitude.CohortDownloadFailure
import com.amplitude.Metrics
import com.amplitude.util.Loader
import com.amplitude.util.logger
import kotlinx.coroutines.coroutineScope

internal class CohortLoader(
    private val maxCohortSize: Int,
    private val cohortApi: CohortApi,
    private val cohortStorage: CohortStorage,
) {
    companion object {
        val log by logger()
    }

    private val loader = Loader()

    suspend fun loadCohorts(cohortIds: Set<String>) =
        coroutineScope {
            for (cohortId in cohortIds.shuffled()) {
                loadCohort(cohortId)
            }
        }

    private suspend fun loadCohort(cohortId: String) {
        log.trace("loadCohort: start - cohortId={}", cohortId)
        loader.load(cohortId) {
            val lockAcquired = cohortStorage.tryLockCohortLoading(cohortId, 300)
            if (!lockAcquired) {
                log.info("loadCohort: cohort {} is already being loaded by another instance, skipping", cohortId)
                return@load
            }
            try {
                try {
                    val storageCohort = cohortStorage.getCohortDescription(cohortId)
                    try {
                        Metrics.with({ CohortDownload }, { e -> CohortDownloadFailure(e) }) {
                            cohortApi.streamCohort(cohortId, storageCohort?.lastModified, maxCohortSize, cohortStorage)
                        }
                        val updated = cohortStorage.getCohortDescription(cohortId)
                        if (updated != null) {
                            log.info("Cohort download/save completed. {}", updated)
                        }
                    } catch (_: CohortNotModifiedException) {
                       log.debug("loadCohort: cohort not modified - cohortId={}", cohortId)
                    }
                } catch (t: Throwable) {
                    // Don't throw if we fail to download the cohort. We
                    // prefer to continue to update flags.
                    log.error("Cohort download/save failed. $cohortId", t)
                }
            } finally {
                // Always release the lock, even if download failed
                cohortStorage.releaseCohortLoadingLock(cohortId)
            }
        }
        log.trace("loadCohort: end - cohortId={}", cohortId)
    }
}
