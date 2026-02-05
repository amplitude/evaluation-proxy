package com.amplitude.exposure

import com.amplitude.util.Cache

internal interface ExposureFilter {
    suspend fun shouldTrack(exposure: Exposure): Boolean
}

internal class InMemoryExposureFilter(size: Int) : ExposureFilter {
    // Cache of canonical exposure to the last sent timestamp.
    private val cache = Cache<String, Unit>(size, DAY_MILLIS)

    override suspend fun shouldTrack(exposure: Exposure): Boolean {
        val canonicalExposure = exposure.canonicalize()
        val track = cache.get(canonicalExposure) == null
        if (track) {
            cache.set(canonicalExposure, Unit)
        }
        return track
    }
}
