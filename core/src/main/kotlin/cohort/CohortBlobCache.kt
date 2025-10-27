package com.amplitude.cohort

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Simple in-memory cache for gzipped cohort blobs.
 * Key: "{cohortId}"
 */
internal object CohortBlobCache {
    private data class CacheEntry(val bytes: ByteArray)

    private val lock = Mutex()
    private val map = HashMap<String, CacheEntry>()

    suspend fun get(key: String): ByteArray? =
        lock.withLock {
            map[key]?.bytes
        }

    suspend fun put(
        key: String,
        bytes: ByteArray,
    ) = lock.withLock {
        map[key] = CacheEntry(bytes)
    }

    suspend fun remove(key: String) =
        lock.withLock {
            map.remove(key)
            null
        }
}
