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
            assertNull(cache.get("c1"))
        }

    @Test
    fun `put then get returns same bytes`() =
        runBlocking {
            val key = "c1"
            val bytes = byteArrayOf(1, 2, 3)
            val cache = CohortBlobCache()
            cache.put(key, bytes)
            val result = cache.get(key)
            assertContentEquals(bytes, result)
        }

    @Test
    fun `remove deletes entry`() =
        runBlocking {
            val key = "c1"
            val bytes = byteArrayOf(1, 2, 3)
            val cache = CohortBlobCache()
            cache.put(key, bytes)
            cache.remove(key)
            assertNull(cache.get(key))
        }
}
