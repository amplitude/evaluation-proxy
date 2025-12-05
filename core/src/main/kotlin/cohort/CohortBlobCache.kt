package com.amplitude.cohort

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Simple in-memory cache for gzipped cohort blobs.
 * Key: "{cohortId}:{lastModified}"
 * Automatically evicts old versions when a new version is cached for the same cohortId.
 */
internal class CohortBlobCache {
    private data class CacheEntry(val bytes: ByteArray)

    private val lock = Mutex()
    private val map = HashMap<String, CacheEntry>()
    private val cachedVersions = HashMap<String, Long>()

    private fun key(
        cohortId: String,
        lastModified: Long,
    ) = "$cohortId:$lastModified"

    suspend fun get(
        cohortId: String,
        lastModified: Long,
    ): ByteArray? =
        lock.withLock {
            map[key(cohortId, lastModified)]?.bytes
        }

    suspend fun put(
        cohortId: String,
        lastModified: Long,
        bytes: ByteArray,
    ) = lock.withLock {
        // Evict old version if exists
        cachedVersions[cohortId]?.let { oldLastModified ->
            map.remove(key(cohortId, oldLastModified))
        }
        cachedVersions[cohortId] = lastModified
        map[key(cohortId, lastModified)] = CacheEntry(bytes)
    }

    suspend fun remove(
        cohortId: String,
        lastModified: Long,
    ) = lock.withLock {
        map.remove(key(cohortId, lastModified))
        if (cachedVersions[cohortId] == lastModified) {
            cachedVersions.remove(cohortId)
        }
    }
}
