package cohort

import com.amplitude.cohort.CohortApi
import com.amplitude.cohort.CohortLoader
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.InMemoryCohortStorage
import io.mockk.coEvery
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class CohortLoaderTest {
    private val maxCohortSize = Int.MAX_VALUE

    @Test
    fun `streaming loader persists computed size from memberIds`(): Unit =
        runBlocking {
            val cohortId = "stream-b"
            val members = (1..5).map { it.toString() }.toSet()
            val api = mockk<CohortApi>()
            val storage: CohortStorage = spyk(InMemoryCohortStorage())
            val loader = CohortLoader(maxCohortSize, api, storage)

            coEvery { api.streamCohort(eq(cohortId), isNull(), eq(maxCohortSize), any()) } coAnswers {
                val storageArg = arg<CohortStorage>(3)
                val desc = com.amplitude.cohort.CohortDescription(cohortId, "User", 0, 100)
                val acc = storageArg.createWriter(desc)
                acc.addMembers(members.toList())
                acc.complete(members.size)
            }

            loader.loadCohorts(setOf(cohortId))
            val stored = storage.getCohort(cohortId)
            assertEquals(members.size, stored?.size)
            assertEquals(members, stored?.members)
        }
}
