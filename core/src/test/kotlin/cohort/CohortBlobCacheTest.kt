package cohort

import com.amplitude.cohort.CohortBlobCache
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertNull

class CohortBlobCacheTest {
    @Test
    fun `get returns null when missing`() =
        runBlocking {
            val cache = CohortBlobCache()
            assertNull(cache.get("c1", 1000L))
        }

    @Test
    fun `put then get returns same bytes`() =
        runBlocking {
            val bytes = byteArrayOf(1, 2, 3)
            val cache = CohortBlobCache()
            cache.put("c1", 1000L, bytes)
            val result = cache.get("c1", 1000L)
            assertContentEquals(bytes, result)
        }

    @Test
    fun `remove deletes entry`() =
        runBlocking {
            val bytes = byteArrayOf(1, 2, 3)
            val cache = CohortBlobCache()
            cache.put("c1", 1000L, bytes)
            cache.remove("c1", 1000L)
            assertNull(cache.get("c1", 1000L))
        }

    @Test
    fun `put automatically evicts old version for same cohortId`() =
        runBlocking {
            val bytesV1 = byteArrayOf(1, 2, 3)
            val bytesV2 = byteArrayOf(4, 5, 6)
            val cache = CohortBlobCache()

            // Cache v1
            cache.put("c1", 1000L, bytesV1)
            assertContentEquals(bytesV1, cache.get("c1", 1000L))

            // Cache v2 - should automatically evict v1
            cache.put("c1", 2000L, bytesV2)

            // v2 should be present
            assertContentEquals(bytesV2, cache.get("c1", 2000L))
            // v1 should be evicted
            assertNull(cache.get("c1", 1000L))
        }

    @Test
    fun `different cohorts are cached independently`() =
        runBlocking {
            val bytesC1 = byteArrayOf(1, 2, 3)
            val bytesC2 = byteArrayOf(4, 5, 6)
            val cache = CohortBlobCache()

            cache.put("c1", 1000L, bytesC1)
            cache.put("c2", 1000L, bytesC2)

            // Both cohorts should be cached independently
            assertContentEquals(bytesC1, cache.get("c1", 1000L))
            assertContentEquals(bytesC2, cache.get("c2", 1000L))
        }

    @Test
    fun `putting same version twice overwrites`() =
        runBlocking {
            val bytes1 = byteArrayOf(1, 2, 3)
            val bytes2 = byteArrayOf(4, 5, 6)
            val cache = CohortBlobCache()

            cache.put("c1", 1000L, bytes1)
            cache.put("c1", 1000L, bytes2)

            // Should have the latest bytes
            assertContentEquals(bytes2, cache.get("c1", 1000L))
        }
}
