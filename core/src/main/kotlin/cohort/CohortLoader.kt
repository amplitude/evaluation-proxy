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
            try {
                val storageCohort = cohortStorage.getCohortDescription(cohortId)
                val cohort =
                    Metrics.with({ CohortDownload }, { e -> CohortDownloadFailure(e) }) {
                        try {
                            cohortApi.getCohort(cohortId, storageCohort?.lastModified, maxCohortSize)
                        } catch (_: CohortNotModifiedException) {
                            log.debug("loadCohort: cohort not modified - cohortId={}", cohortId)
                            null
                        }
                    }
                if (cohort != null) {
                    log.info("Cohort download complete. {}", cohort)
                    cohortStorage.putCohort(cohort)
                }
            } catch (t: Throwable) {
                // Don't throw if we fail to download the cohort. We
                // prefer to continue to update flags.
                log.error("Cohort download failed. $cohortId", t)
            }
        }
        log.trace("loadCohort: end - cohortId={}", cohortId)
    }
}
