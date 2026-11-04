package deployment

import com.amplitude.deployment.RedisDeploymentStorage
import com.amplitude.util.redis.RedisKey
import io.mockk.coVerify
import io.mockk.spyk
import kotlinx.coroutines.runBlocking
import test.InMemoryRedis
import test.flag
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals

class DeploymentStorageFlagCacheTest {
    private val deploymentKey = "deployment"
    private val flagsKey = RedisKey.FlagConfigs("amplitude", "12345", deploymentKey)

    private class FakeClock(var nanos: Long = 0) {
        fun advanceMillis(millis: Long) {
            nanos += TimeUnit.MILLISECONDS.toNanos(millis)
        }
    }

    private fun storage(
        redis: InMemoryRedis,
        ttlMillis: Long,
        clock: FakeClock,
    ) = RedisDeploymentStorage("amplitude", "12345", redis, redis, ttlMillis) { clock.nanos }

    @Test
    fun `cached read is served from memory within ttl`(): Unit =
        runBlocking {
            val redis = spyk(InMemoryRedis())
            val clock = FakeClock()
            val storage = storage(redis, 5000, clock)
            storage.putFlag(deploymentKey, flag("a"))

            repeat(10) { assertEquals(setOf("a"), storage.getAllFlagsCached(deploymentKey).keys) }
            coVerify(exactly = 1) { redis.hgetall(flagsKey) }
        }

    @Test
    fun `cached read reloads after ttl and sees writes from other instances`(): Unit =
        runBlocking {
            val redis = InMemoryRedis()
            val clock = FakeClock()
            val replicaA = storage(redis, 5000, clock)
            val replicaB = storage(redis, 5000, clock)
            replicaA.putFlag(deploymentKey, flag("a"))
            assertEquals(setOf("a"), replicaA.getAllFlagsCached(deploymentKey).keys)

            // Another instance writes; replica A keeps serving its cached copy within the TTL.
            replicaB.putFlag(deploymentKey, flag("b"))
            clock.advanceMillis(4999)
            assertEquals(setOf("a"), replicaA.getAllFlagsCached(deploymentKey).keys)

            // After the TTL the write is visible.
            clock.advanceMillis(1)
            assertEquals(setOf("a", "b"), replicaA.getAllFlagsCached(deploymentKey).keys)
        }

    @Test
    fun `local writes invalidate the cache immediately`(): Unit =
        runBlocking {
            val redis = InMemoryRedis()
            val clock = FakeClock()
            val storage = storage(redis, 5000, clock)
            storage.putFlag(deploymentKey, flag("a"))
            assertEquals(setOf("a"), storage.getAllFlagsCached(deploymentKey).keys)

            storage.putFlag(deploymentKey, flag("b"))
            assertEquals(setOf("a", "b"), storage.getAllFlagsCached(deploymentKey).keys)

            storage.removeFlag(deploymentKey, "a")
            assertEquals(setOf("b"), storage.getAllFlagsCached(deploymentKey).keys)

            storage.removeAllFlags(deploymentKey)
            assertEquals(emptySet(), storage.getAllFlagsCached(deploymentKey).keys)

            storage.putFlag(deploymentKey, flag("c"))
            assertEquals(setOf("c"), storage.getAllFlagsCached(deploymentKey).keys)
            storage.removeDeployment(deploymentKey)
            assertEquals(emptySet(), storage.getAllFlagsCached(deploymentKey).keys)
        }

    @Test
    fun `uncached read always goes to redis`(): Unit =
        runBlocking {
            val redis = spyk(InMemoryRedis())
            val clock = FakeClock()
            val storage = storage(redis, 5000, clock)
            storage.putFlag(deploymentKey, flag("a"))
            storage.getAllFlagsCached(deploymentKey)

            repeat(3) { storage.getAllFlags(deploymentKey) }
            coVerify(exactly = 4) { redis.hgetall(flagsKey) }
        }

    @Test
    fun `ttl of zero disables the cache`(): Unit =
        runBlocking {
            val redis = spyk(InMemoryRedis())
            val clock = FakeClock()
            val storage = storage(redis, 0, clock)
            storage.putFlag(deploymentKey, flag("a"))

            repeat(3) { storage.getAllFlagsCached(deploymentKey) }
            coVerify(exactly = 3) { redis.hgetall(flagsKey) }
        }
}
