package com.amplitude.assignment

import com.amplitude.util.Cache

internal interface AssignmentFilter {
    suspend fun shouldTrack(assignment: Assignment): Boolean
}

internal class InMemoryAssignmentFilter(size: Int) : AssignmentFilter {

    // Cache of canonical assignment to the last sent timestamp.
    private val cache = Cache<String, Unit>(size, DAY_MILLIS)

    override suspend fun shouldTrack(assignment: Assignment): Boolean {
        val canonicalAssignment = assignment.canonicalize()
        val track = cache.get(canonicalAssignment) == null
        if (track) {
            cache.remove(canonicalAssignment)
        }
        return track
    }
}
