import com.amplitude.assignment.Assignment
import com.amplitude.assignment.InMemoryAssignmentFilter
import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.util.toEvaluationContext
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test
import test.user

class AssignmentFilterTest {
    @Test
    fun `test single assignment`() =
        runBlocking {
            val filter = InMemoryAssignmentFilter(100)
            val assignment =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(assignment))
        }

    @Test
    fun `test duplicate assignments`() =
        runBlocking {
            val filter = InMemoryAssignmentFilter(100)
            val assignment1 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            filter.shouldTrack(assignment1)
            val assignment2 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertFalse(filter.shouldTrack(assignment2))
        }

    @Test
    fun `test same user different results`() =
        runBlocking {
            val filter = InMemoryAssignmentFilter(100)
            val assignment1 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(assignment1))
            val assignment2 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "control"),
                        "flag-key-2" to EvaluationVariant(key = "on"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(assignment2))
        }

    @Test
    fun `test same results for different users`() =
        runBlocking {
            val filter = InMemoryAssignmentFilter(100)
            val assignment1 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(assignment1))
            val assignment2 =
                Assignment(
                    user(userId = "different user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(assignment2))
        }

    @Test
    fun `test empty results`() =
        runBlocking {
            val filter = InMemoryAssignmentFilter(100)
            val assignment1 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(),
                )
            Assert.assertTrue(filter.shouldTrack(assignment1))
            val assignment2 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(),
                )
            Assert.assertFalse(filter.shouldTrack(assignment2))
            val assignment3 =
                Assignment(
                    user(userId = "different user").toEvaluationContext(),
                    mapOf(),
                )
            Assert.assertTrue(filter.shouldTrack(assignment3))
        }

    @Test
    fun `test duplicate assignments with different result ordering`() =
        runBlocking {
            val filter = InMemoryAssignmentFilter(100)
            val assignment1 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    linkedMapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(assignment1))
            val assignment2 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    linkedMapOf(
                        "flag-key-2" to EvaluationVariant(key = "control"),
                        "flag-key-1" to EvaluationVariant(key = "on"),
                    ),
                )
            Assert.assertFalse(filter.shouldTrack(assignment2))
        }

    @Test
    fun `test lru replacement`() =
        runBlocking {
            val filter = InMemoryAssignmentFilter(2)
            val assignment1 =
                Assignment(
                    user(userId = "user").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(assignment1))
            val assignment2 =
                Assignment(
                    user(userId = "user2").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(assignment2))
            val assignment3 =
                Assignment(
                    user(userId = "user3").toEvaluationContext(),
                    mapOf(
                        "flag-key-1" to EvaluationVariant(key = "on"),
                        "flag-key-2" to EvaluationVariant(key = "control"),
                    ),
                )
            Assert.assertTrue(filter.shouldTrack(assignment3))
            Assert.assertTrue(filter.shouldTrack(assignment1))
        }
}
