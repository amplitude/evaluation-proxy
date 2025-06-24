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

    @Test
    fun `new flag with cohorts loads cohorts and stores flag`(): Unit =
        runBlocking {
            val api = mockk<DeploymentApi>()
            val storage = spyk(InMemoryDeploymentStorage())
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val flag = flag(flagKey = flagKey, cohortIds = cohortIds, metadata = mapOf("flagVersion" to 1L))
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(flag)
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } returns Unit
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 1) { storage.putFlag(eq(deploymentKey), eq(flag)) }
            assertEquals(flag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `network flag with newer version than storage flag loads cohorts and stores flag`(): Unit =
        runBlocking {
            val existingFlag = flag(flagKey = flagKey, cohortIds = setOf("a", "b"), metadata = mapOf("flagVersion" to 1L))
            val api = mockk<DeploymentApi>()
            val storage =
                spyk(
                    InMemoryDeploymentStorage().apply {
                        putFlag(deploymentKey, existingFlag)
                    },
                )
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val newerNetworkFlag = flag(flagKey = flagKey, cohortIds = cohortIds, metadata = mapOf("flagVersion" to 2L))
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(newerNetworkFlag)
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } returns Unit
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 1) { storage.putFlag(eq(deploymentKey), eq(newerNetworkFlag)) }
            assertEquals(newerNetworkFlag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `network flag with same version as storage flag skips processing`(): Unit =
        runBlocking {
            val existingFlag = flag(flagKey = flagKey, cohortIds = setOf("a", "b"), metadata = mapOf("flagVersion" to 1L))
            val api = mockk<DeploymentApi>()
            val storage =
                spyk(
                    InMemoryDeploymentStorage().apply {
                        putFlag(deploymentKey, existingFlag)
                    },
                )
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val sameVersionFlag = flag(flagKey = flagKey, cohortIds = cohortIds, metadata = mapOf("flagVersion" to 1L))
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(sameVersionFlag)
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 0) { cohortLoader.loadCohorts(allAny()) }
            coVerify(exactly = 0) { storage.putFlag(allAny(), allAny()) }
            // The existing flag should remain unchanged
            assertEquals(existingFlag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `network flag with older version than storage flag skips processing`(): Unit =
        runBlocking {
            val existingFlag = flag(flagKey = flagKey, cohortIds = setOf("a", "b"), metadata = mapOf("flagVersion" to 2L))
            val api = mockk<DeploymentApi>()
            val storage =
                spyk(
                    InMemoryDeploymentStorage().apply {
                        putFlag(deploymentKey, existingFlag)
                    },
                )
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val olderNetworkFlag = flag(flagKey = flagKey, cohortIds = cohortIds, metadata = mapOf("flagVersion" to 1L))
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(olderNetworkFlag)
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 0) { cohortLoader.loadCohorts(allAny()) }
            coVerify(exactly = 0) { storage.putFlag(allAny(), allAny()) }
            // The existing flag should remain unchanged
            assertEquals(existingFlag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `new flag without cohorts skips cohort loading and stores flag`(): Unit =
        runBlocking {
            val api = mockk<DeploymentApi>()
            val storage = spyk(InMemoryDeploymentStorage())
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = emptySet<String>()
            val flag = flag(flagKey = flagKey, cohortIds = cohortIds, metadata = mapOf("flagVersion" to 1L))
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(flag)
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 0) { cohortLoader.loadCohorts(allAny()) }
            coVerify(exactly = 1) { storage.putFlag(eq(deploymentKey), eq(flag)) }
            assertEquals(flag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `network flag without cohorts but newer version stores flag without loading cohorts`(): Unit =
        runBlocking {
            val existingFlag = flag(flagKey = flagKey, cohortIds = emptySet(), metadata = mapOf("flagVersion" to 1L))
            val api = mockk<DeploymentApi>()
            val storage =
                spyk(
                    InMemoryDeploymentStorage().apply {
                        putFlag(deploymentKey, existingFlag)
                    },
                )
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = emptySet<String>()
            val newerNetworkFlag = flag(flagKey = flagKey, cohortIds = cohortIds, metadata = mapOf("flagVersion" to 2L))
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(newerNetworkFlag)
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 0) { cohortLoader.loadCohorts(allAny()) }
            coVerify(exactly = 1) { storage.putFlag(eq(deploymentKey), eq(newerNetworkFlag)) }
            assertEquals(newerNetworkFlag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `network flag with no version metadata treated as newest and loads cohorts`(): Unit =
        runBlocking {
            val api = mockk<DeploymentApi>()
            val storage = spyk(InMemoryDeploymentStorage())
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val networkFlag = flag(flagKey = flagKey, cohortIds = cohortIds, metadata = null)
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(networkFlag)
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } returns Unit
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 1) { storage.putFlag(eq(deploymentKey), eq(networkFlag)) }
            assertEquals(networkFlag, storage.getFlag(deploymentKey, flagKey))
        }

    @Test
    fun `network flag with version beats storage flag with no version and loads cohorts`(): Unit =
        runBlocking {
            val existingFlag = flag(flagKey = flagKey, cohortIds = setOf("a", "b"), metadata = null)
            val api = mockk<DeploymentApi>()
            val storage =
                spyk(
                    InMemoryDeploymentStorage().apply {
                        putFlag(deploymentKey, existingFlag)
                    },
                )
            val cohortLoader = mockk<CohortLoader>()
            val cohortIds = setOf("a", "b")
            val versionedNetworkFlag = flag(flagKey = flagKey, cohortIds = cohortIds, metadata = mapOf("flagVersion" to 1L))
            coEvery { api.getFlagConfigs(eq(deploymentKey)) } returns listOf(versionedNetworkFlag)
            coEvery { cohortLoader.loadCohorts(eq(cohortIds)) } returns Unit
            val loader = DeploymentLoader(api, storage, cohortLoader)
            loader.loadDeployment(deploymentKey)
            coVerify(exactly = 1) { api.getFlagConfigs(eq(deploymentKey)) }
            coVerify(exactly = 0) { storage.removeFlag(allAny(), allAny()) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
            coVerify(exactly = 1) { storage.putFlag(eq(deploymentKey), eq(versionedNetworkFlag)) }
            assertEquals(versionedNetworkFlag, storage.getFlag(deploymentKey, flagKey))
        }
}
