import com.amplitude.util.Cache
import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test

class CacheTest {

    @Test
    fun `test get no entry`() =
        runBlocking {
            val cache = Cache<Int, Int>(4)
            val value = cache.get(0)
            Assert.assertNull(value)
        }

    @Test
    fun `test set and get`() =
        runBlocking {
            val cache = Cache<Int, Int>(4)
            cache.set(0, 0)
            val value = cache.get(0)
            Assert.assertEquals(0, value)
        }

    @Test
    fun `test least recently used entry is removed`() =
        runBlocking {
            val cache = Cache<Int, Int>(4)
            repeat(4) { i ->
                cache.set(i, i)
            }
            cache.set(4, 4)
            val value = cache.get(0)
            Assert.assertNull(value)
        }

    @Test
    fun `test first set then get entry is not removed`() =
        runBlocking {
            val cache = Cache<Int, Int>(4)
            repeat(4) { i ->
                cache.set(i, i)
            }
            val expectedValue = cache.get(0)
            cache.set(4, 4)
            val actualValue = cache.get(0)
            Assert.assertEquals(expectedValue, actualValue)
            val removedValue = cache.get(1)
            Assert.assertNull(removedValue)
        }

    @Test
    fun `test first set then re-set entry is not removed`() =
        runBlocking {
            val cache = Cache<Int, Int>(4)
            repeat(4) { i ->
                cache.set(i, i)
            }
            cache.set(0, 0)
            cache.set(4, 4)
            val actualValue = cache.get(0)
            Assert.assertEquals(0, actualValue)
            val removedValue = cache.get(1)
            Assert.assertNull(removedValue)
        }

    @Test
    fun `test first set then re-set with different value entry is not removed`() =
        runBlocking {
            val cache = Cache<Int, Int>(4)
            repeat(4) { i ->
                cache.set(i, i)
            }
            cache.set(0, 100)
            cache.set(4, 4)
            val actualValue = cache.get(0)
            Assert.assertEquals(100, actualValue)
            val removedValue = cache.get(1)
            Assert.assertNull(removedValue)
        }

    @Test
    fun `test concurrent access`() =
        runBlocking {
            val n = 100
            val cache = Cache<Int, Int>(n)
            val jobs = mutableListOf<Job>()
            repeat(n) { i ->
                jobs +=
                    launch {
                        cache.set(i, i)
                    }
            }
            jobs.joinAll()
            repeat(n) { i ->
                Assert.assertEquals(i, cache.get(i))
            }
            jobs.clear()
            val k = 50
            repeat(k) { i ->
                jobs +=
                    launch {
                        cache.set(i + k, i + k)
                    }
            }
            jobs.joinAll()
            repeat(k) { i ->
                Assert.assertEquals(i, cache.get(i))
                Assert.assertEquals(i + k, cache.get(i + k))
            }
        }
}
