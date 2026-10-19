package util.redis

import com.amplitude.util.redis.Redis
import com.amplitude.util.redis.RedisClusterConnection
import com.amplitude.util.redis.RedisConnection
import com.amplitude.util.redis.RedisKey
import io.lettuce.core.RedisClient
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assume.assumeTrue
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/** Opt in with dedicated disposable Redis instances; never point these tests at shared data. */
class RedisUnlinkIntegrationTest {
    @Test
    fun `standalone client unlinks sets and blobs`(): Unit =
        runBlocking {
            val uri = System.getenv("REDIS_TEST_URI")
            assumeTrue("REDIS_TEST_URI is not configured", !uri.isNullOrBlank())
            checkUnlink(RedisConnection(uri!!))
        }

    @Test
    fun `cluster client unlinks sets and blobs`(): Unit =
        runBlocking {
            val uri = System.getenv("REDIS_CLUSTER_TEST_URI")
            assumeTrue("REDIS_CLUSTER_TEST_URI is not configured", !uri.isNullOrBlank())
            checkUnlink(RedisClusterConnection(uri!!))
        }

    @Test
    fun `expired member sets are freed asynchronously with the documented server setting`(): Unit =
        runBlocking {
            val uri = System.getenv("REDIS_TEST_URI")
            assumeTrue("REDIS_TEST_URI is not configured", !uri.isNullOrBlank())
            val client = RedisClient.create(uri!!)
            val connection = client.connect()
            val commands = connection.sync()
            val key = "evaluation-proxy-expiry-test:${UUID.randomUUID()}"
            try {
                assertEquals("yes", commands.configGet("lazyfree-lazy-expire")["lazyfree-lazy-expire"])
                val before = lazyfreedObjects(commands.info("memory"))
                commands.sadd(key, *(1..20_000).map { "user$it" }.toTypedArray())
                commands.expire(key, 1)
                withTimeout(10_000) {
                    while (commands.exists(key) != 0L || lazyfreedObjects(commands.info("memory")) <= before) {
                        delay(10)
                    }
                }
            } finally {
                commands.unlink(key)
                connection.close()
                client.shutdown()
            }
        }

    private suspend fun checkUnlink(redis: Redis) {
        val prefix = "evaluation-proxy-unlink-test:${UUID.randomUUID()}"
        val members = RedisKey.CohortMembers(prefix, "project", "cohort", "User", 1)
        val blob = RedisKey.CohortBlob(prefix, "project", "cohort", 1)
        try {
            redis.sadd(members, (1..20_000).map { "user$it" }.toSet())
            redis.expire(members, 60.seconds)
            redis.set(blob, "test-blob")
            redis.expire(blob, 60.seconds)
            assertEquals(20_000L, redis.scard(members))
            assertEquals("test-blob", redis.get(blob))
            redis.unlink(members)
            redis.unlink(blob)
            assertEquals(0L, redis.scard(members))
            assertNull(redis.get(blob))
            redis.unlink(members)
            redis.unlink(blob)
        } finally {
            redis.unlink(members)
            redis.unlink(blob)
        }
    }

    private fun lazyfreedObjects(info: String): Long {
        val value = info.lineSequence().first { it.startsWith("lazyfreed_objects:") }.substringAfter(':').trim().toLong()
        assertTrue(value >= 0)
        return value
    }
}
