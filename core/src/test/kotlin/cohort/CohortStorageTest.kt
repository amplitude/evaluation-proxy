package cohort

import test.InMemoryRedis
import test.cohort
import com.amplitude.cohort.Cohort
import com.amplitude.cohort.CohortDescription
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.cohort.RedisCohortStorage
import com.amplitude.cohort.toCohortDescription
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Duration

class CohortStorageTest {

    private val redis = InMemoryRedis()

    @Test
    fun `test in memory`(): Unit = runBlocking {
        test(InMemoryCohortStorage())
    }

    @Test
    fun `test redis`(): Unit = runBlocking {
        test(RedisCohortStorage("12345", Duration.INFINITE, "amplitude ", redis, redis))
    }

    private fun test(cohortStorage: CohortStorage): Unit = runBlocking {
        val cohortA = cohort("a")
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
        // test put, get, cohort
        cohortStorage.putCohort(cohortA)
        cohort = cohortStorage.getCohort(cohortA.id)
        assertEquals(cohortA, cohort)
        // test get description, description
        description = cohortStorage.getCohortDescription(cohortA.id)
        assertEquals(cohortA.toCohortDescription(), description)
        // test put, get all, cohorts
        cohortStorage.putCohort(cohortB)
        cohorts = cohortStorage.getCohorts()
        assertEquals(
            mapOf(
                cohortA.id to cohortA,
                cohortB.id to cohortB
            ),
            cohorts
        )
        // test get descriptions, descriptions
        descriptions = cohortStorage.getCohortDescriptions()
        assertEquals(
            mapOf(
                cohortA.id to cohortA.toCohortDescription(),
                cohortB.id to cohortB.toCohortDescription()
            ),
            descriptions
        )
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
        // test delete other
        cohortStorage.deleteCohort(cohortB.toCohortDescription())
        // test get all, empty
        cohorts = cohortStorage.getCohorts()
        assertEquals(0, cohorts.size)
        // test get descriptions, empty
        descriptions = cohortStorage.getCohortDescriptions()
        assertEquals(0, descriptions.size)
    }
}
