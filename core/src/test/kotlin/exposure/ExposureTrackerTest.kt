package exposure

import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.exposure.DAY_MILLIS
import com.amplitude.exposure.Exposure
import com.amplitude.exposure.toAmplitudeEvents
import com.amplitude.util.toEvaluationContext
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test
import test.user

class ExposureTrackerTest {
    @Test
    fun `test exposure to amplitude events - creates per-flag events`() =
        runBlocking {
            val context = user(userId = "user", deviceId = "device").toEvaluationContext()
            val results =
                mapOf(
                    "flag-key-1" to
                        EvaluationVariant(
                            key = "on",
                            metadata =
                                mapOf(
                                    "deployed" to true,
                                    "segmentName" to "Segment",
                                    "flagVersion" to 13,
                                ),
                        ),
                    "flag-key-2" to
                        EvaluationVariant(
                            key = "control",
                            metadata =
                                mapOf(
                                    "deployed" to true,
                                    "segmentName" to "All Other Users",
                                    "flagVersion" to 12,
                                ),
                        ),
                )
            val exposure = Exposure(context, results)
            val events = exposure.toAmplitudeEvents()

            // Should create one event per flag
            Assert.assertEquals(2, events.size)

            for (event in events) {
                Assert.assertEquals("[Experiment] Exposure", event.eventType)
                Assert.assertEquals("user", event.userId)
                Assert.assertEquals("device", event.deviceId)

                val flagKey = event.eventProperties?.getString("[Experiment] Flag Key")
                Assert.assertNotNull(flagKey)
                Assert.assertEquals(results[flagKey]?.key, event.eventProperties?.getString("[Experiment] Variant"))
            }
        }

    @Test
    fun `test exposure skips default variants`() =
        runBlocking {
            val context = user(userId = "user", deviceId = "device").toEvaluationContext()
            val results =
                mapOf(
                    "flag-key-1" to
                        EvaluationVariant(
                            key = "on",
                            metadata = mapOf("deployed" to true),
                        ),
                    "flag-key-2" to
                        EvaluationVariant(
                            key = "off",
                            metadata = mapOf("deployed" to true, "default" to true),
                        ),
                )
            val exposure = Exposure(context, results)
            val events = exposure.toAmplitudeEvents()

            // Should only create event for non-default flag
            Assert.assertEquals(1, events.size)
            Assert.assertEquals("flag-key-1", events[0].eventProperties?.getString("[Experiment] Flag Key"))
        }

    @Test
    fun `test exposure insert id includes flag key`() =
        runBlocking {
            val context = user(userId = "user", deviceId = "device").toEvaluationContext()
            val results =
                mapOf(
                    "flag-key-1" to
                        EvaluationVariant(
                            key = "on",
                            metadata = mapOf("deployed" to true),
                        ),
                )
            val exposure = Exposure(context, results)
            val events = exposure.toAmplitudeEvents()

            // Insert ID should include flag key in the hash
            val canonicalization = "user device flag-key-1 on "
            val hash = ("flag-key-1 $canonicalization").hashCode()
            val day = exposure.timestamp / DAY_MILLIS
            val expected = "user device $hash $day"
            Assert.assertEquals(expected, events[0].insertId)
        }

    @Test
    fun `test exposure with no eligible variants returns empty list`() =
        runBlocking {
            val context = user(userId = "user", deviceId = "device").toEvaluationContext()
            val results =
                mapOf(
                    "flag-key-1" to
                        EvaluationVariant(
                            key = "off",
                            metadata = mapOf("deployed" to true, "default" to true),
                        ),
                    "flag-key-2" to
                        EvaluationVariant(
                            key = "control",
                            metadata = mapOf("deployed" to true, "default" to true),
                        ),
                )
            val exposure = Exposure(context, results)
            val events = exposure.toAmplitudeEvents()

            // Both flags should be skipped (both are default variants)
            Assert.assertEquals(0, events.size)
        }

    @Test
    fun `test exposure skips non-deployed variants`() =
        runBlocking {
            val context = user(userId = "user", deviceId = "device").toEvaluationContext()
            val results =
                mapOf(
                    "flag-key-1" to
                        EvaluationVariant(
                            key = "on",
                            metadata = mapOf("deployed" to true),
                        ),
                    "flag-key-2" to
                        EvaluationVariant(
                            key = "control",
                            metadata = mapOf("deployed" to false),
                        ),
                    "flag-key-3" to
                        EvaluationVariant(
                            key = "treatment",
                            // No deployed metadata - should be skipped
                        ),
                )
            val exposure = Exposure(context, results)
            val events = exposure.toAmplitudeEvents()

            // Should only create event for deployed flag
            Assert.assertEquals(1, events.size)
            Assert.assertEquals("flag-key-1", events[0].eventProperties?.getString("[Experiment] Flag Key"))
        }

    @Test
    fun `test exposure skips mutual exclusion group user properties`() =
        runBlocking {
            val context = user(userId = "user", deviceId = "device").toEvaluationContext()
            val results =
                mapOf(
                    "flag-key-1" to
                        EvaluationVariant(
                            key = "on",
                            metadata = mapOf("deployed" to true, "flagType" to "mutual-exclusion-group"),
                        ),
                )
            val exposure = Exposure(context, results)
            val events = exposure.toAmplitudeEvents()

            Assert.assertEquals(1, events.size)
            // User properties $set should be empty for mutual exclusion groups
            val setProps = events[0].userProperties?.getJSONObject("\$set")
            Assert.assertEquals(0, setProps?.length())
        }

    @Test
    fun `test exposure sets user properties for non-mutual-exclusion-group`() =
        runBlocking {
            val context = user(userId = "user", deviceId = "device").toEvaluationContext()
            val results =
                mapOf(
                    "flag-key-1" to
                        EvaluationVariant(
                            key = "on",
                            metadata = mapOf("deployed" to true, "flagType" to "experiment"),
                        ),
                )
            val exposure = Exposure(context, results)
            val events = exposure.toAmplitudeEvents()

            Assert.assertEquals(1, events.size)
            // User properties $set should contain the flag assignment
            val setProps = events[0].userProperties?.getJSONObject("\$set")
            Assert.assertEquals("on", setProps?.getString("[Experiment] flag-key-1"))
        }
}
