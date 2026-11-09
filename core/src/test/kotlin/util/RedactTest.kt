package util

import com.amplitude.Configuration
import com.amplitude.RedisConfiguration
import com.amplitude.util.redacted
import com.amplitude.util.redactedUri
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RedactTest {
    @Test
    fun `redacted keeps only last four characters`() {
        assertEquals("****5678", "server-12345678".redacted())
        assertEquals("****", "abc".redacted())
        assertEquals("null", (null as String?).redacted())
    }

    @Test
    fun `redactedUri strips user info`() {
        assertEquals("redis://[redacted]@host:6379/0", "redis://user:secret@host:6379/0".redactedUri())
        assertEquals("redis://host:6379", "redis://host:6379".redactedUri())
        assertEquals(null, (null as String?).redactedUri())
    }

    @Test
    fun `configuration toString does not contain redis password`() {
        val configuration =
            Configuration(
                redis =
                    RedisConfiguration(
                        uri = "redis://default:hunter2@redis:6379",
                        readOnlyUri = "redis://default:hunter2@replica:6379",
                    ),
            )
        val printed = configuration.toString()
        assertFalse(printed.contains("hunter2"), printed)
        assertTrue(printed.contains("redis://[redacted]@redis:6379"), printed)
        assertTrue(printed.contains("redis://[redacted]@replica:6379"), printed)
    }

    @Test
    fun `configuration zone EU derives all urls`() {
        val configuration = Configuration(serverZone = "EU")
        assertEquals("https://flag.lab.eu.amplitude.com", configuration.serverUrl)
        assertEquals("https://cohort-v2.lab.eu.amplitude.com", configuration.cohortServerUrl)
        assertEquals("https://experiment.eu.amplitude.com", configuration.managementServerUrl)
        assertEquals("https://api.eu.amplitude.com/2/httpapi", configuration.analyticsServerUrl)
    }
}
