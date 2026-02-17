package exposure

import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.exposure.Exposure
import com.amplitude.exposure.InMemoryExposureFilter
import com.amplitude.util.toEvaluationContext
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test
import test.user

class ExposureFilterTest {
    @Test
    fun `test single exposure`() =
        runBlocking {
            val filter = InMemoryExposureFilter(100)
            val exposure =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(exposure))
        }

    @Test
    fun `test duplicate exposures`() =
        runBlocking {
            val filter = InMemoryExposureFilter(100)
            val exposure1 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            filter.shouldTrack(exposure1)
            val exposure2 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertFalse(filter.shouldTrack(exposure2))
        }

    @Test
    fun `test same user different results`() =
        runBlocking {
            val filter = InMemoryExposureFilter(100)
            val exposure1 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(exposure1))
            val exposure2 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "control"),
                        "flag-key-2" to EvaluationVariant(key = "on"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(exposure2))
        }

    @Test
    fun `test same results for different users`() =
        runBlocking {
            val filter = InMemoryExposureFilter(100)
            val exposure1 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(exposure1))
            val exposure2 =
                Exposure(
                    user(userId = "different user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(exposure2))
        }

    @Test
    fun `test empty results`() =
        runBlocking {
            val filter = InMemoryExposureFilter(100)
            val exposure1 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(),
                )
            Assert.assertTrue(filter.shouldTrack(exposure1))
            val exposure2 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(),
                )
            Assert.assertFalse(filter.shouldTrack(exposure2))
            val exposure3 =
                Exposure(
                    user(userId = "different user").toEvaluationContext(),
                    mapOf(),
                )
            Assert.assertTrue(filter.shouldTrack(exposure3))
        }

    @Test
    fun `test duplicate exposures with different result ordering`() =
        runBlocking {
            val filter = InMemoryExposureFilter(100)
            val exposure1 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    linkedMapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(exposure1))
            val exposure2 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    linkedMapOf(
                        "flag-key-2" to EvaluationVariant(key = "control"),
                        "flag-key-1" to EvaluationVariant(key = "on"),
                    ),
                )
            Assert.assertFalse(filter.shouldTrack(exposure2))
        }

    @Test
    fun `test lru replacement`() =
        runBlocking {
            val filter = InMemoryExposureFilter(2)
            val exposure1 =
                Exposure(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(exposure1))
            val exposure2 =
                Exposure(
                    user(userId = "user2").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(exposure2))
            val exposure3 =
                Exposure(
                    user(userId = "user3").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(exposure3))
            Assert.assertTrue(filter.shouldTrack(exposure1))
        }
}
