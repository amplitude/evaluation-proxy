package com.amplitude.cohort

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface CohortStorage {
    suspend fun getCohortDescription(cohortId: String): CohortDescription?
    suspend fun getCohortMembershipsForUser(userId: String, cohortIds: Set<String>? = null): Set<String>
    suspend fun putCohort(description: CohortDescription, members: Set<String>)
}

class InMemoryCohortStorage : CohortStorage {

    private class Cohort(
        val description: CohortDescription,
        val members: Set<String>
    )

    private val lock = Mutex()
    private val cohorts = mutableMapOf<String, Cohort>()

    override suspend fun getCohortDescription(cohortId: String): CohortDescription? {
        return lock.withLock { cohorts[cohortId]?.description }
    }

    override suspend fun putCohort(description: CohortDescription, members: Set<String>) {
        return lock.withLock { cohorts[description.id] = Cohort(description, members) }
    }

    override suspend fun getCohortMembershipsForUser(userId: String, cohortIds: Set<String>?): Set<String> {
        return lock.withLock {
            (cohortIds ?: cohorts.keys).mapNotNull { id ->
                when (cohorts[id]?.members?.contains(userId)) {
                    true -> id
                    else -> null
                }
            }.toSet()
        }
    }
}
