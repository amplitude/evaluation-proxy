package cohort

import com.amplitude.cohort.CohortBlobCache
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertNull

class CohortBlobCacheTest {
    @Test
    fun `get returns null when missing`() = runBlocking {
        assertNull(CohortBlobCache.get("c1"))
    }

    @Test
    fun `put then get returns same bytes`() = runBlocking {
        val key = "c1"
        val bytes = byteArrayOf(1, 2, 3)
        CohortBlobCache.put(key, bytes)
        val result = CohortBlobCache.get(key)
        assertContentEquals(bytes, result)
    }

    @Test
    fun `remove deletes entry`() = runBlocking {
        val key = "c1"
        val bytes = byteArrayOf(1, 2, 3)
        CohortBlobCache.put(key, bytes)
        CohortBlobCache.remove(key)
        assertNull(CohortBlobCache.get(key))
    }
}



