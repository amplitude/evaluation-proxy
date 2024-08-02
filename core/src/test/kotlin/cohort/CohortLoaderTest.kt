package cohort

import com.amplitude.cohort.Cohort
import com.amplitude.cohort.CohortApi
import com.amplitude.cohort.CohortLoader
import com.amplitude.cohort.CohortNotModifiedException
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.util.HttpErrorException
import io.ktor.http.HttpStatusCode
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class CohortLoaderTest {
    private val maxCohortSize = Int.MAX_VALUE

    @Test
    fun `load cohorts, success`(): Unit =
        runBlocking {
            val cohortA = Cohort("a", "User", 1, 100, setOf("1"))
            val cohortB = Cohort("b", "User", 1, 100, setOf("1"))
            val cohortC = Cohort("c", "User", 1, 100, setOf("1"))
            val api = mockk<CohortApi>()
            val storage: CohortStorage = spyk(InMemoryCohortStorage())
            val loader = CohortLoader(maxCohortSize, api, storage)
            coEvery { api.getCohort("a", null, maxCohortSize) } returns cohortA
            coEvery { api.getCohort("b", null, maxCohortSize) } returns cohortB
            coEvery { api.getCohort("c", null, maxCohortSize) } returns cohortC
            // Run 1
            loader.loadCohorts(setOf("a", "b", "c"))
            coVerify(exactly = 3) { api.getCohort(allAny(), isNull(), eq(maxCohortSize)) }
            assertEquals(
                mapOf(
                    "a" to cohortA,
                    "b" to cohortB,
                    "c" to cohortC,
                ),
                storage.getCohorts(),
            )
            coVerify(exactly = 3) { storage.putCohort(allAny()) }
            // Run 2
            val cohortB2 = cohortB.copy(size = 2, lastModified = 200, members = setOf("1", "2"))
            coEvery { api.getCohort("a", 100, maxCohortSize) } returns cohortA
            coEvery { api.getCohort("b", 100, maxCohortSize) } returns cohortB2
            coEvery { api.getCohort("c", 100, maxCohortSize) } throws CohortNotModifiedException("c")
            loader.loadCohorts(setOf("a", "b", "c"))
            coVerify(exactly = 3) { api.getCohort(allAny(), eq(100), eq(maxCohortSize)) }
            assertEquals(
                mapOf(
                    "a" to cohortA,
                    "b" to cohortB2,
                    "c" to cohortC,
                ),
                storage.getCohorts(),
            )
            // Cohort C should not be stored
            coVerify(exactly = 5) { storage.putCohort(allAny()) }
        }

    @Test
    fun `load cohorts, simultaneous loading of the same cohorts, only downloads once per cohort`(): Unit =
        runBlocking {
            val cohortA = Cohort("a", "User", 1, 100, setOf("1"))
            val cohortB = Cohort("b", "User", 1, 100, setOf("1"))
            val cohortC = Cohort("c", "User", 1, 100, setOf("1"))
            val api = mockk<CohortApi>()
            val storage: CohortStorage = spyk(InMemoryCohortStorage())
            val loader = CohortLoader(maxCohortSize, api, storage)
            coEvery { api.getCohort("a", null, maxCohortSize) } coAnswers {
                delay(100)
                cohortA
            }
            coEvery { api.getCohort("b", null, maxCohortSize) } coAnswers {
                delay(100)
                cohortB
            }
            coEvery { api.getCohort("c", null, maxCohortSize) } coAnswers {
                delay(100)
                cohortC
            }
            val j1 =
                launch {
                    loader.loadCohorts(setOf("a", "b", "c"))
                }
            val j2 =
                launch {
                    loader.loadCohorts(setOf("a", "b", "c"))
                }
            listOf(j1, j2).joinAll()
            coVerify(exactly = 3) { api.getCohort(allAny(), isNull(), eq(maxCohortSize)) }
            assertEquals(
                mapOf(
                    "a" to cohortA,
                    "b" to cohortB,
                    "c" to cohortC,
                ),
                storage.getCohorts(),
            )
        }

    @Test
    fun `load cohorts, failure, failed cohort not stored, does not throw`(): Unit =
        runBlocking {
            val cohortA = Cohort("a", "User", 1, 100, setOf("1"))
            val cohortC = Cohort("c", "User", 1, 100, setOf("1"))
            val api = mockk<CohortApi>()
            val storage: CohortStorage = spyk(InMemoryCohortStorage())
            val loader = CohortLoader(maxCohortSize, api, storage)
            coEvery { api.getCohort("a", null, maxCohortSize) } returns cohortA
            coEvery { api.getCohort("b", null, maxCohortSize) } throws HttpErrorException(HttpStatusCode.InternalServerError)
            coEvery { api.getCohort("c", null, maxCohortSize) } returns cohortC
            // Run 1
            loader.loadCohorts(setOf("a", "b", "c"))
            coVerify(exactly = 3) { api.getCohort(allAny(), isNull(), eq(maxCohortSize)) }
            assertEquals(
                mapOf(
                    "a" to cohortA,
                    "c" to cohortC,
                ),
                storage.getCohorts(),
            )
        }
}
