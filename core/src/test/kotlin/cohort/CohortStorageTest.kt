package cohort

import com.amplitude.cohort.Cohort
import com.amplitude.cohort.CohortBlobCache
import com.amplitude.cohort.CohortDescription
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.GetCohortResponse
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.cohort.RedisCohortStorage
import com.amplitude.cohort.toCohortDescription
import com.amplitude.util.json
import com.amplitude.util.redis.RedisKey
import kotlinx.coroutines.runBlocking
import test.InMemoryRedis
import test.cohort
import java.io.ByteArrayInputStream
import java.util.Base64
import java.util.zip.GZIPInputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
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
            test(RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis, 1000, 1000, CohortBlobCache()))
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
            val cohortStorage = RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis, 1000, 1000, CohortBlobCache())
            // put cohort via writer
            val cohort = cohort("a", lastModified = 1, members = setOf("1", "2", "3"))
            run {
                val acc = cohortStorage.createWriter(cohort.toCohortDescription())
                acc.addMembers(cohort.members.toList())
                acc.complete(cohort.members.size)
            }
            // blob is stored and decodes correctly
            run {
                val blobKey = RedisKey.CohortBlob("amplitude ", "12345", cohort.id, cohort.lastModified)
                val b64 = redis.get(blobKey)
                assertNotNull(b64)
                val gz = Base64.getDecoder().decode(b64)
                val jsonStr = GZIPInputStream(ByteArrayInputStream(gz)).use { String(it.readBytes(), Charsets.UTF_8) }
                val parsed = json.decodeFromString<GetCohortResponse>(jsonStr)
                assertEquals(GetCohortResponse.fromCohort(cohort), parsed)
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
            // cache invalidated (next get loads new blob and caches it again)
            run {
                val blobKey2 = RedisKey.CohortBlob("amplitude ", "12345", cohort.id, 2)
                val b64 = redis.get(blobKey2)
                assertNotNull(b64)
            }
            // old blob and members keys should be marked for expiration on update
            run {
                val oldBlobKey = RedisKey.CohortBlob("amplitude ", "12345", cohort.id, 1)
                assertTrue(redis.expirations.containsKey(oldBlobKey.value))
                // existing cohort members key (lastModified=1) should also be set to expire
                val oldMembersKey = RedisKey.CohortMembers("amplitude ", "12345", cohort.id, "User", 1)
                assertTrue(redis.expirations.containsKey(oldMembersKey.value))
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
    fun `test redis, cohort update diffs client side across partitions`(): Unit =
        runBlocking {
            // diffPartitionMaxMembers=3 forces multiple hash partitions for a 10-member cohort so
            // the partitioned code path is exercised, not just the single-partition fast path.
            val cohortStorage =
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
                )
            val v1 = cohort("p", lastModified = 1, members = (1..10).map { "u$it" }.toSet())
            run {
                val acc = cohortStorage.createWriter(v1.toCohortDescription())
                acc.addMembers(v1.members.toList())
                acc.complete(v1.members.size)
            }
            for (member in 1..10) {
                assertEquals(setOf("p"), cohortStorage.getCohortMemberships("User", "u$member"))
            }
            // v2 removes u1-u5 and adds u11-u15
            val v2 = cohort("p", lastModified = 2, members = (6..15).map { "u$it" }.toSet())
            run {
                val acc = cohortStorage.createWriter(v2.toCohortDescription())
                acc.addMembers(v2.members.toList())
                acc.complete(v2.members.size)
            }
            for (removed in 1..5) {
                assertEquals(emptySet(), cohortStorage.getCohortMemberships("User", "u$removed"))
            }
            for (retained in 6..15) {
                assertEquals(setOf("p"), cohortStorage.getCohortMemberships("User", "u$retained"))
            }
        }

    @Test
    fun `test redis, cohort update uses sdiffstore path by default`(): Unit =
        runBlocking {
            // Same update scenario as the streamed-diff test, but with the flag left at its
            // default (off) so the server-side SDIFFSTORE path is exercised.
            val cohortStorage =
                RedisCohortStorage(
                    "12345",
                    Duration.INFINITE,
                    "amplitude ",
                    redis,
                    redis,
                    1000,
                    1000,
                    CohortBlobCache(),
                )
            val v1 = cohort("p", lastModified = 1, members = (1..10).map { "u$it" }.toSet())
            run {
                val acc = cohortStorage.createWriter(v1.toCohortDescription())
                acc.addMembers(v1.members.toList())
                acc.complete(v1.members.size)
            }
            val v2 = cohort("p", lastModified = 2, members = (6..15).map { "u$it" }.toSet())
            run {
                val acc = cohortStorage.createWriter(v2.toCohortDescription())
                acc.addMembers(v2.members.toList())
                acc.complete(v2.members.size)
            }
            for (removed in 1..5) {
                assertEquals(emptySet(), cohortStorage.getCohortMemberships("User", "u$removed"))
            }
            for (retained in 6..15) {
                assertEquals(setOf("p"), cohortStorage.getCohortMemberships("User", "u$retained"))
            }
        }

    @Test
    fun `test redis, streamed diff flushes adds and removals above chunk threshold`(): Unit =
        runBlocking {
            // >1000 additions and >1000 removals in a single partition so the incremental
            // flush branches (added/removed buffers reaching diffScanChunkSize) are exercised.
            val cohortStorage =
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
                )
            val v1 = cohort("p", lastModified = 1, members = (1..2500).map { "u$it" }.toSet())
            run {
                val acc = cohortStorage.createWriter(v1.toCohortDescription())
                acc.addMembers(v1.members.toList())
                acc.complete(v1.members.size)
            }
            // v2 removes u1-u1300 and adds u2501-u3800
            val v2 = cohort("p", lastModified = 2, members = (1301..3800).map { "u$it" }.toSet())
            run {
                val acc = cohortStorage.createWriter(v2.toCohortDescription())
                acc.addMembers(v2.members.toList())
                acc.complete(v2.members.size)
            }
            for (removed in 1..1300) {
                assertEquals(emptySet(), cohortStorage.getCohortMemberships("User", "u$removed"))
            }
            for (retained in 1301..3800) {
                assertEquals(setOf("p"), cohortStorage.getCohortMemberships("User", "u$retained"))
            }
        }

    @Test
    fun `test redis, rejects non-positive diff tuning values`() {
        assertFailsWith<IllegalArgumentException> {
            RedisCohortStorage(
                "12345", Duration.INFINITE, "amplitude ", redis, redis, 1000, 1000, CohortBlobCache(),
                diffPartitionMaxMembers = 0,
            )
        }
        assertFailsWith<IllegalArgumentException> {
            RedisCohortStorage(
                "12345", Duration.INFINITE, "amplitude ", redis, redis, 1000, 1000, CohortBlobCache(),
                diffPartitionMaxMembers = -2_000_000,
            )
        }
        assertFailsWith<IllegalArgumentException> {
            RedisCohortStorage(
                "12345", Duration.INFINITE, "amplitude ", redis, redis, 1000, 1000, CohortBlobCache(),
                diffScanChunkSize = 0,
            )
        }
    }

    @Test
    fun `test redis, streamed diff falls back to all additions when existing members key is missing`(): Unit =
        runBlocking {
            val cohortStorage =
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
                )
            val v1 = cohort("p", lastModified = 1, members = (1..5).map { "u$it" }.toSet())
            run {
                val acc = cohortStorage.createWriter(v1.toCohortDescription())
                acc.addMembers(v1.members.toList())
                acc.complete(v1.members.size)
            }
            // Simulate the published version's members key expiring/being deleted out from
            // under the diff.
            redis.del(RedisKey.CohortMembers("amplitude ", "12345", "p", "User", 1))
            val v2 = cohort("p", lastModified = 2, members = (3..7).map { "u$it" }.toSet())
            run {
                val acc = cohortStorage.createWriter(v2.toCohortDescription())
                acc.addMembers(v2.members.toList())
                acc.complete(v2.members.size)
            }
            // The refresh must still publish the new version...
            assertEquals(2L, cohortStorage.getCohortDescription("p")?.lastModified)
            // ...with every new member present.
            for (added in 3..7) {
                assertEquals(setOf("p"), cohortStorage.getCohortMemberships("User", "u$added"))
            }
            // Members dropped between versions keep a stale membership in this degraded state
            // (matching SDIFFSTORE semantics for a missing existing key).
            for (stale in 1..2) {
                assertEquals(setOf("p"), cohortStorage.getCohortMemberships("User", "u$stale"))
            }
        }

    @Test
    fun `test redis, put large cohort, no OutOfMemoryError`(): Unit =
        runBlocking {
            val cohortStorage = RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis, 1000, 1000, CohortBlobCache())
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

    @Test
    fun `pending version set carries a ttl during ingestion, cleared on promotion`(): Unit =
        runBlocking {
            val cohortStorage = RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis, 1000, 1000, CohortBlobCache())
            val cohortV1 = cohort("a", lastModified = 1, members = setOf("1", "2"))
            val membersKey = RedisKey.CohortMembers("amplitude ", "12345", cohortV1.id, "User", 1)

            val acc = cohortStorage.createWriter(cohortV1.toCohortDescription())
            acc.addMembers(cohortV1.members.toList())
            // pending (un-promoted) version set carries a TTL so an aborted refresh self-cleans
            assertTrue(redis.expirations.containsKey(membersKey.value))

            acc.complete(cohortV1.members.size)
            // promotion clears the TTL so the current version never expires
            assertFalse(redis.expirations.containsKey(membersKey.value))
        }

    @Test
    fun `unpromoted refresh leaves previous version live and new version self-cleaning`(): Unit =
        runBlocking {
            val cohortStorage = RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis, 1000, 1000, CohortBlobCache())
            // v1 promoted
            val cohortV1 = cohort("a", lastModified = 1, members = setOf("1", "2"))
            run {
                val acc = cohortStorage.createWriter(cohortV1.toCohortDescription())
                acc.addMembers(cohortV1.members.toList())
                acc.complete(cohortV1.members.size)
            }
            // v2 refresh downloads members but never completes (simulates a mid-flight failure)
            val cohortV2 = cohort("a", lastModified = 2, members = setOf("1", "3"))
            cohortStorage.createWriter(cohortV2.toCohortDescription()).addMembers(cohortV2.members.toList())

            val v1Key = RedisKey.CohortMembers("amplitude ", "12345", cohortV1.id, "User", 1)
            val v2Key = RedisKey.CohortMembers("amplitude ", "12345", cohortV2.id, "User", 2)
            // the published (current) version is untouched by the failed refresh
            assertFalse(redis.expirations.containsKey(v1Key.value))
            // the stranded pending version will self-clean via its TTL
            assertTrue(redis.expirations.containsKey(v2Key.value))
            // published cohort still reads correctly
            assertEquals(cohortV1, cohortStorage.getCohort(cohortV1.id))
        }
}
