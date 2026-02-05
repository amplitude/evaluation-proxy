package com.amplitude.assignment

import com.amplitude.util.Cache

@Deprecated(
    message = "Assignment service is deprecated. Use ExposureFilter with Exposure service instead.",
    replaceWith = ReplaceWith("com.amplitude.exposure.ExposureFilter"),
)
internal interface AssignmentFilter {
    suspend fun shouldTrack(assignment: Assignment): Boolean
}

@Deprecated(
    message = "Assignment service is deprecated. Use InMemoryExposureFilter with Exposure service instead.",
    replaceWith = ReplaceWith("com.amplitude.exposure.InMemoryExposureFilter"),
)
internal class InMemoryAssignmentFilter(size: Int) : AssignmentFilter {
    // Cache of canonical assignment to the last sent timestamp.
    private val cache = Cache<String, Unit>(size, DAY_MILLIS)

    override suspend fun shouldTrack(assignment: Assignment): Boolean {
        val canonicalAssignment = assignment.canonicalize()
        val track = cache.get(canonicalAssignment) == null
        if (track) {
            cache.set(canonicalAssignment, Unit)
        }
        return track
    }
}
