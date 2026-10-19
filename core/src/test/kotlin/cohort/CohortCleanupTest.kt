package cohort

import com.amplitude.cohort.CohortBlobCache
import com.amplitude.cohort.RedisCohortStorage
import com.amplitude.cohort.toCohortDescription
import com.amplitude.util.redis.Redis
import com.amplitude.util.redis.RedisKey
import kotlinx.coroutines.runBlocking
import test.InMemoryRedis
import test.cohort
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

class CohortCleanupTest {
    @Test
    fun `refresh retains old keys for the grace period and deletion unlinks current keys`(): Unit =
        runBlocking {
            for (streamedDiff in listOf(false, true)) {
                val backing = InMemoryRedis()
                val unlinked = mutableListOf<RedisKey>()
                val redis = recordingRedis(backing, unlinked)
                val storage = storage(redis, streamedDiff)
                val v1 = cohort("cohort", lastModified = 1, members = setOf("user1", "user2"))
                val v2 = cohort("cohort", lastModified = 2, members = setOf("user2", "user3"))
                for (version in listOf(v1, v2)) {
                    val writer = storage.createWriter(version.toCohortDescription())
                    writer.addMembers(version.members.toList())
                    writer.complete(version.members.size)
                }

                val oldMembers = RedisKey.CohortMembers("test", "project", "cohort", "User", 1)
                val oldBlob = RedisKey.CohortBlob("test", "project", "cohort", 1)
                val currentMembers = RedisKey.CohortMembers("test", "project", "cohort", "User", 2)
                val currentBlob = RedisKey.CohortBlob("test", "project", "cohort", 2)
                assertEquals(v1.members, backing.smembers(oldMembers))
                assertNotNull(backing.get(oldBlob))
                assertEquals(2.minutes, backing.expirations[oldMembers.value])
                assertEquals(2.minutes, backing.expirations[oldBlob.value])
                assertFalse(backing.expirations.containsKey(currentMembers.value))
                assertEquals(v2, storage.getCohort("cohort"))
                assertEquals(if (streamedDiff) 0 else 2, unlinked.size)
                for (key in unlinked) {
                    assertTrue(key is RedisKey.CohortTemporary)
                    assertEquals(0L, backing.scard(key))
                }

                unlinked.clear()
                storage.deleteCohort(v2.toCohortDescription())
                assertEquals(setOf(currentBlob, currentMembers), unlinked.toSet())
                assertNull(storage.getCohort("cohort"))
                assertEquals(0L, backing.scard(currentMembers))
                assertNull(backing.get(currentBlob))
                assertEquals(emptySet(), storage.getCohortMemberships("User", "user2"))
                assertEquals(emptySet(), storage.getCohortMemberships("User", "user3"))
                assertEquals(v1.members, backing.smembers(oldMembers))
            }
        }

    @Test
    fun `failed legacy diff unlinks temporary keys and preserves the published version`(): Unit =
        runBlocking {
            val backing = InMemoryRedis()
            val unlinked = mutableListOf<RedisKey>()
            val recording = recordingRedis(backing, unlinked)
            var diffCalls = 0
            val redis =
                object : Redis by recording {
                    override suspend fun sdiffstore(
                        destKey: RedisKey,
                        key1: RedisKey,
                        key2: RedisKey,
                    ): Long {
                        if (++diffCalls == 2) error("injected diff failure")
                        return backing.sdiffstore(destKey, key1, key2)
                    }
                }
            val storage = storage(redis, false)
            val v1 = cohort("cohort", lastModified = 1, members = setOf("user1", "user2"))
            val v2 = cohort("cohort", lastModified = 2, members = setOf("user2", "user3"))
            val initial = storage.createWriter(v1.toCohortDescription())
            initial.addMembers(v1.members.toList())
            initial.complete(v1.members.size)
            val refresh = storage.createWriter(v2.toCohortDescription())
            refresh.addMembers(v2.members.toList())
            assertFailsWith<IllegalStateException> { refresh.complete(v2.members.size) }

            assertEquals(v1, storage.getCohort("cohort"))
            assertEquals(2, unlinked.size)
            for (key in unlinked) {
                assertTrue(key is RedisKey.CohortTemporary)
                assertEquals(0L, backing.scard(key))
                assertFalse(backing.expirations.containsKey(key.value))
            }
            val oldMembers = RedisKey.CohortMembers("test", "project", "cohort", "User", 1)
            assertFalse(backing.expirations.containsKey(oldMembers.value))
        }

    private fun recordingRedis(
        backing: InMemoryRedis,
        unlinked: MutableList<RedisKey>,
    ): Redis =
        object : Redis by backing {
            override suspend fun del(key: RedisKey) {
                error("cohort cleanup must not use synchronous DEL")
            }

            override suspend fun unlink(key: RedisKey) {
                unlinked += key
                backing.unlink(key)
            }
        }

    private fun storage(
        redis: Redis,
        streamedDiff: Boolean,
    ) = RedisCohortStorage("project", 2.minutes, "test", redis, redis, 1000, 1000, CohortBlobCache(), streamedDiff)
}
