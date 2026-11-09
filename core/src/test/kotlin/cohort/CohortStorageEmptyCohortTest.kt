package cohort

import com.amplitude.cohort.CohortBlobCache
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.cohort.RedisCohortStorage
import com.amplitude.cohort.toCohortDescription
import kotlinx.coroutines.runBlocking
import test.InMemoryRedis
import test.cohort
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration

/**
 * Regression tests: a cohort version with zero members must remove the memberships of
 * every member of the previous version, and a later non-empty version must not resurrect them.
 */
class CohortStorageEmptyCohortTest {
    @Test
    fun `in memory, emptied cohort removes memberships`(): Unit =
        runBlocking {
            test(InMemoryCohortStorage())
        }

    @Test
    fun `redis sdiffstore path, emptied cohort removes memberships`(): Unit =
        runBlocking {
            val redis = InMemoryRedis()
            test(RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis, 1000, 1000, CohortBlobCache()))
        }

    @Test
    fun `redis streamed diff path, emptied cohort removes memberships`(): Unit =
        runBlocking {
            val redis = InMemoryRedis()
            test(
                RedisCohortStorage(
                    "12345",
                    Duration.INFINITE,
                    "amplitude ",
                    redis,
                    redis,
                    1000,
                    1000,
                    CohortBlobCache(),
                    streamedDiffEnabled = true,
                    diffPartitionMaxMembers = 3,
                ),
            )
        }

    private suspend fun test(cohortStorage: CohortStorage) {
        // v1: u1..u10 are members
        val v1 = cohort("x", lastModified = 1, members = (1..10).map { "u$it" }.toSet())
        cohortStorage.createWriter(v1.toCohortDescription()).apply {
            addMembers(v1.members.toList())
            complete(v1.members.size)
        }
        for (m in 1..10) {
            assertEquals(setOf("x"), cohortStorage.getCohortMemberships("User", "u$m"), "v1 member u$m")
        }

        // v2: cohort emptied in Amplitude
        val v2 = cohort("x", lastModified = 2, members = emptySet())
        cohortStorage.createWriter(v2.toCohortDescription()).complete(0)
        for (m in 1..10) {
            assertEquals(emptySet(), cohortStorage.getCohortMemberships("User", "u$m"), "v2 former member u$m")
        }
        val v2Description = cohortStorage.getCohortDescription("x")
        assertEquals(2L, v2Description?.lastModified)
        assertEquals(0, v2Description?.size)

        // v3: one new member; the former members must stay removed
        val v3 = cohort("x", lastModified = 3, members = setOf("u11"))
        cohortStorage.createWriter(v3.toCohortDescription()).apply {
            addMembers(v3.members.toList())
            complete(v3.members.size)
        }
        assertEquals(setOf("x"), cohortStorage.getCohortMemberships("User", "u11"))
        for (m in 1..10) {
            assertEquals(emptySet(), cohortStorage.getCohortMemberships("User", "u$m"), "v3 former member u$m")
        }
    }
}
