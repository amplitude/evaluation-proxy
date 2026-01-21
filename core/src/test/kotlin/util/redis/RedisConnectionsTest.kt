package util.redis

import com.amplitude.util.redis.parseReadFrom
import io.lettuce.core.ReadFrom
import org.junit.Assert.assertEquals
import org.junit.Test

class RedisConnectionsTest {
    @Test
    fun `parseReadFrom handles ANY`() {
        val result = parseReadFrom("ANY")
        assertEquals(ReadFrom.ANY, result)
    }

    @Test
    fun `parseReadFrom handles any case insensitive`() {
        val result1 = parseReadFrom("any")
        val result2 = parseReadFrom("Any")
        val result3 = parseReadFrom("aNy")
        assertEquals(ReadFrom.ANY, result1)
        assertEquals(ReadFrom.ANY, result2)
        assertEquals(ReadFrom.ANY, result3)
    }

    @Test
    fun `parseReadFrom handles REPLICA_PREFERRED`() {
        val result = parseReadFrom("REPLICA_PREFERRED")
        assertEquals(ReadFrom.REPLICA_PREFERRED, result)
    }

    @Test
    fun `parseReadFrom handles replica_preferred case insensitive`() {
        val result1 = parseReadFrom("replica_preferred")
        val result2 = parseReadFrom("Replica_Preferred")
        assertEquals(ReadFrom.REPLICA_PREFERRED, result1)
        assertEquals(ReadFrom.REPLICA_PREFERRED, result2)
    }

    @Test
    fun `parseReadFrom defaults to ANY for invalid values`() {
        val result1 = parseReadFrom("INVALID")
        val result2 = parseReadFrom("MASTER")
        val result3 = parseReadFrom("")
        val result4 = parseReadFrom("replica")
        assertEquals(ReadFrom.ANY, result1)
        assertEquals(ReadFrom.ANY, result2)
        assertEquals(ReadFrom.ANY, result3)
        assertEquals(ReadFrom.ANY, result4)
    }
}
