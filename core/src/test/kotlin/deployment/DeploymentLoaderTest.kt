package deployment

import com.amplitude.cohort.CohortLoader
import com.amplitude.deployment.DeploymentApi
import com.amplitude.deployment.DeploymentLoader
import com.amplitude.deployment.InMemoryDeploymentStorage
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertNull
import test.flag
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

class DeploymentLoaderTest {
    private val deploymentKey = "deployment"
    private val flagKey = "flag"

    @Test
    fun `load flags without cohorts, cohorts not loaded, success`(): Unit =
        runBlocking {
            val api = mockk<DeploymentApi>()
            val storage = spyk(InMemoryDeploymentStorage())
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = emptySet<String>()
            val flag = flag(flagKey = flagKey, cohortIds = cohortIds)
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(flag)
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 0) { cohortLoader.loadCohorts(allAny()) }
            coVerify(exactly = 1) { storage.putFlag(allAny(), allAny()) }
            assertEquals(flag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `load flags with cohorts, cohorts are loaded, success`(): Unit =
        runBlocking {
            val api = mockk<DeploymentApi>()
            val storage = spyk(InMemoryDeploymentStorage())
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val flag = flag(flagKey = flagKey, cohortIds = cohortIds)
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(flag)
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } returns Unit
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 1) { storage.putFlag(allAny(), allAny()) }
            assertEquals(flag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `existing flags state, some flags removed, success`(): Unit =
        runBlocking {
            val existingFlagKey = "existing"
            val existingFlag = flag(existingFlagKey)
            val api = mockk<DeploymentApi>()
            val storage =
                spyk(
                    InMemoryDeploymentStorage().apply {
                        putFlag(deploymentKey, existingFlag)
                    },
                )
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val flag = flag(flagKey = flagKey, cohortIds = cohortIds)
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(flag)
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } returns Unit
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 1) { storage.removeFlag(eq(deploymentKey), eq(existingFlagKey)) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 1) { storage.putFlag(allAny(), allAny()) }
            assertEquals(flag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `getFlagConfigs fails, throws`(): Unit =
        runBlocking {
            val api = mockk<DeploymentApi>()
            val storage = spyk(InMemoryDeploymentStorage())
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } throws RuntimeException("fail")
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } returns Unit
            val loader = DeploymentLoader(api, storage, cohortLoader)
            try {
                loader.loadDeployment(deploymentKey)
                fail("Expected loadDeployment to throw exception")
            } catch (e: RuntimeException) {
                // Success
            }
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 0) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 0) { storage.putFlag(allAny(), allAny()) }
            assertNull(storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `removeFlag fails, throws`(): Unit =
        runBlocking {
            val existingFlagKey = "existing"
            val existingFlag = flag(existingFlagKey)
            val api = mockk<DeploymentApi>()
            val storage =
                spyk(
                    InMemoryDeploymentStorage().apply {
                        putFlag(deploymentKey, existingFlag)
                    },
                )
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val flag = flag(flagKey = flagKey, cohortIds = cohortIds)
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(flag)
            coEvery { storage.removeFlag(eq(deploymentKey), eq(existingFlagKey)) } throws RuntimeException("fail")
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } returns Unit
            val loader = DeploymentLoader(api, storage, cohortLoader)
            try {
                loader.loadDeployment(deploymentKey)
                fail("Expected loadDeployment to throw exception")
            } catch (e: RuntimeException) {
                // Success
            }
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 1) { storage.removeFlag(eq(deploymentKey), eq(existingFlagKey)) }
            coVerify(exactly = 0) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 0) { storage.putFlag(allAny(), allAny()) }
            assertNull(storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `loadCohorts fails, throws`(): Unit =
        runBlocking {
            val api = mockk<DeploymentApi>()
            val storage = spyk(InMemoryDeploymentStorage())
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val flag = flag(flagKey = flagKey, cohortIds = cohortIds)
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(flag)
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } throws RuntimeException("fail")
            val loader = DeploymentLoader(api, storage, cohortLoader)
            try {
                loader.loadDeployment(deploymentKey)
                fail("Expected loadDeployment to throw exception")
            } catch (e: RuntimeException) {
                // Success
            }
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 0) { storage.putFlag(allAny(), allAny()) }
            assertNull(storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `putFlag fails, throws`(): Unit =
        runBlocking {
            val api = mockk<DeploymentApi>()
            val storage = spyk(InMemoryDeploymentStorage())
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val flag = flag(flagKey = flagKey, cohortIds = cohortIds)
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(flag)
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } returns Unit
            coEvery { storage.putFlag(eq(deploymentKey), eq(flag)) } throws RuntimeException("fail")
            val loader = DeploymentLoader(api, storage, cohortLoader)
            try {
                loader.loadDeployment(deploymentKey)
                fail("Expected loadDeployment to throw exception")
            } catch (e: RuntimeException) {
                // Success
            }
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 1) { storage.putFlag(allAny(), allAny()) }
            assertNull(storage.getFlag(deploymentKey, flagKey))
        }
}
