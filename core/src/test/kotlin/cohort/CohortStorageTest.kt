package cohort

import com.amplitude.cohort.Cohort
import com.amplitude.cohort.CohortDescription
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.cohort.RedisCohortStorage
import com.amplitude.cohort.toCohortDescription
import com.amplitude.util.redis.RedisKey
import kotlinx.coroutines.runBlocking
import test.InMemoryRedis
import test.cohort
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Duration

class CohortStorageTest {
    private val redis = InMemoryRedis()

    @Test
    fun `test in memory`(): Unit =
        runBlocking {
            test(InMemoryCohortStorage())
        }

    @Test
    fun `test redis`(): Unit =
        runBlocking {
            test(RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis, 1000))
        }

    private fun test(cohortStorage: CohortStorage): Unit =
        runBlocking {
            val groupType = "User"
            val groupName = "1"
            val cohortA = cohort("a", groupType = groupType, members = setOf(groupName))
            val cohortB = cohort("b")

            // test get, null
            var cohort: Cohort? = cohortStorage.getCohort(cohortA.id)
            assertNull(cohort)
            // test get all, empty
            var cohorts: Map<String, Cohort> = cohortStorage.getCohorts()
            assertEquals(0, cohorts.size)
            // test get description, null
            var description: CohortDescription? = cohortStorage.getCohortDescription(cohortA.id)
            assertNull(description)
            // test get descriptions, empty
            var descriptions: Map<String, CohortDescription> = cohortStorage.getCohortDescriptions()
            assertEquals(0, descriptions.size)
            // test get memberships, empty
            var memberships: Set<String> = cohortStorage.getCohortMemberships(groupType, groupName)
            assertEquals(0, memberships.size)
            // seed via writer
            run {
                val acc = cohortStorage.createWriter(cohortA.toCohortDescription())
                acc.addMembers(cohortA.members.toList())
                acc.complete(cohortA.members.size)
            }
            cohort = cohortStorage.getCohort(cohortA.id)
            assertEquals(cohortA, cohort)
            // test get description, description
            description = cohortStorage.getCohortDescription(cohortA.id)
            assertEquals(cohortA.toCohortDescription(), description)
            // test get memberships, memberships
            memberships = cohortStorage.getCohortMemberships(groupType, groupName)
            assertEquals(setOf(cohortA.id), memberships)
            // seed via writer
            run {
                val acc = cohortStorage.createWriter(cohortB.toCohortDescription())
                acc.addMembers(cohortB.members.toList())
                acc.complete(cohortB.members.size)
            }
            cohorts = cohortStorage.getCohorts()
            assertEquals(
                mapOf(
                    cohortA.id to cohortA,
                    cohortB.id to cohortB,
                ),
                cohorts,
            )
            // test get descriptions, descriptions
            descriptions = cohortStorage.getCohortDescriptions()
            assertEquals(
                mapOf(
                    cohortA.id to cohortA.toCohortDescription(),
                    cohortB.id to cohortB.toCohortDescription(),
                ),
                descriptions,
            )
            // test get memberships, memberships
            memberships = cohortStorage.getCohortMemberships(groupType, groupName)
            assertEquals(setOf(cohortA.id, cohortB.id), memberships)
            // test delete one
            cohortStorage.deleteCohort(cohortA.toCohortDescription())
            // test get deleted, null
            cohort = cohortStorage.getCohort(cohortA.id)
            assertNull(cohort)
            // test get other, cohort
            cohort = cohortStorage.getCohort(cohortB.id)
            assertEquals(cohortB, cohort)
            // test get all, cohort
            cohorts = cohortStorage.getCohorts()
            assertEquals(mapOf(cohortB.id to cohortB), cohorts)
            // test get description deleted, null
            description = cohortStorage.getCohortDescription(cohortA.id)
            assertNull(description)
            // test get description other, description
            description = cohortStorage.getCohortDescription(cohortB.id)
            assertEquals(cohortB.toCohortDescription(), description)
            // test get descriptions, description
            descriptions = cohortStorage.getCohortDescriptions()
            assertEquals(mapOf(cohortB.id to cohortB.toCohortDescription()), descriptions)
            // test get memberships, membership other
            memberships = cohortStorage.getCohortMemberships(groupType, groupName)
            assertEquals(setOf(cohortB.id), memberships)
            // test delete other
            cohortStorage.deleteCohort(cohortB.toCohortDescription())
            // test get all, empty
            cohorts = cohortStorage.getCohorts()
            assertEquals(0, cohorts.size)
            // test get descriptions, empty
            descriptions = cohortStorage.getCohortDescriptions()
            assertEquals(0, descriptions.size)
            // test get memberships, empty
            memberships = cohortStorage.getCohortMemberships(groupType, groupName)
            assertEquals(0, memberships.size)
        }

    @Test
    fun `test redis, put cohort, users memberships exist in redis`(): Unit =
        runBlocking {
            val cohortStorage = RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis, 1000)
            // put cohort via writer
            val cohort = cohort("a", lastModified = 1, members = setOf("1", "2", "3"))
            run {
                val acc = cohortStorage.createWriter(cohort.toCohortDescription())
                acc.addMembers(cohort.members.toList())
                acc.complete(cohort.members.size)
            }
            // check cohort membership
            redis.sscan(RedisKey.UserCohortMemberships("amplitude ", "12345", "User", "1"), 1000)?.let {
                assertEquals(setOf(cohort.id), it)
            }
            // put updated cohort via writer
            run {
                val cohort2 = cohort("a", lastModified = 2, members = setOf("1", "2"))
                val acc = cohortStorage.createWriter(cohort2.toCohortDescription())
                acc.addMembers(cohort2.members.toList())
                acc.complete(cohort2.members.size)
            }
            // check cohort membership exists
            redis.sscan(RedisKey.UserCohortMemberships("amplitude ", "12345", "User", "1"), 1000)?.let {
                assertEquals(setOf(cohort.id), it)
            }
            // check cohort membership removed
            redis.sscan(RedisKey.UserCohortMemberships("amplitude ", "12345", "User", "3"), 1000)?.let {
                assertEquals(0, it.size)
            }
            // delete cohort
            cohortStorage.deleteCohort(cohort.toCohortDescription())
            // check cohort membership
            redis.sscan(RedisKey.UserCohortMemberships("amplitude ", "12345", "User", "1"), 1000)?.let {
                assertEquals(0, it.size)
            }
            redis.sscan(RedisKey.UserCohortMemberships("amplitude ", "12345", "User", "2"), 1000)?.let {
                assertEquals(0, it.size)
            }
            redis.sscan(RedisKey.UserCohortMemberships("amplitude ", "12345", "User", "3"), 1000)?.let {
                assertEquals(0, it.size)
            }
        }

    @Test
    fun `test redis, put large cohort, no OutOfMemoryError`(): Unit =
        runBlocking {
            val cohortStorage = RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis, 1000)
            // Create a large cohort with 10,000 members to simulate memory pressure
            val largeMembers = (1..10000).map { "user_$it" }.toSet()
            val largeCohort = cohort("large", lastModified = 1, members = largeMembers)

            // This should not throw OutOfMemoryError due to batching
            run {
                val acc = cohortStorage.createWriter(largeCohort.toCohortDescription())
                // stream in chunks
                largeMembers.chunked(1000).forEach { acc.addMembers(it) }
                acc.complete(largeMembers.size)
            }

            // Verify the cohort was stored correctly
            val retrievedCohort = cohortStorage.getCohort("large")
            assertEquals(largeCohort, retrievedCohort)
            assertEquals(largeMembers.size, retrievedCohort?.members?.size)
        }

    @Test
    fun `stream writer stores final size from member count`(): Unit =
        runBlocking {
            val storage: CohortStorage = InMemoryCohortStorage()
            val description = CohortDescription(id = "s1", groupType = "User", size = 0, lastModified = 42)
            val acc = storage.createWriter(description)
            acc.addMembers(listOf("u1", "u2"))
            acc.addMembers(listOf("u3"))
            acc.complete(3)
            val stored = storage.getCohort("s1")
            assertEquals(3, stored?.size)
            assertEquals(setOf("u1", "u2", "u3"), stored?.members)
        }
}
