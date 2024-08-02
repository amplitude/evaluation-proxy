package deployment

import com.amplitude.Configuration
import com.amplitude.cohort.CohortLoader
import com.amplitude.deployment.DeploymentLoader
import com.amplitude.deployment.DeploymentRunner
import com.amplitude.deployment.DeploymentStorage
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Test
import test.flag

class DeploymentRunnerTest {
    private val deploymentKey = "deployment"

    @Test
    fun `start, stop, load deployment called once, success`(): Unit =
        runBlocking {
            val flag = flag(cohortIds = setOf("a"))
            val configuration =
                Configuration(
                    flagSyncIntervalMillis = 50,
                    cohortSyncIntervalMillis = 50,
                )
            val cohortLoader = mockk<CohortLoader>()
            val deploymentStorage = mockk<DeploymentStorage>()
            val deploymentLoader = mockk<DeploymentLoader>()
            coEvery { deploymentLoader.loadDeployment(eq(deploymentKey)) } returns Unit
            coEvery { deploymentStorage.getAllFlags(eq(deploymentKey)) } returns mapOf(flag.key to flag)
            coEvery { cohortLoader.loadCohorts(allAny()) } returns Unit
            val deploymentRunner =
                DeploymentRunner(
                    configuration,
                    deploymentKey,
                    cohortLoader,
                    deploymentStorage,
                    deploymentLoader,
                )
            deploymentRunner.start()
            deploymentRunner.stop()
            delay(100)
            coVerify(exactly = 1) { deploymentLoader.loadDeployment(allAny()) }
            coVerify(exactly = 0) { deploymentStorage.getAllFlags(allAny()) }
            coVerify(exactly = 0) { cohortLoader.loadCohorts(allAny()) }
        }

    @Test
    fun `start, delay, periodic loaders run, success`(): Unit =
        runBlocking {
            val cohortIds = setOf("a")
            val flag = flag(cohortIds = cohortIds)
            val configuration =
                Configuration(
                    flagSyncIntervalMillis = 50,
                    cohortSyncIntervalMillis = 50,
                )
            val cohortLoader = mockk<CohortLoader>()
            val deploymentStorage = mockk<DeploymentStorage>()
            val deploymentLoader = mockk<DeploymentLoader>()
            coEvery { deploymentLoader.loadDeployment(eq(deploymentKey)) } returns Unit
            coEvery { deploymentStorage.getAllFlags(eq(deploymentKey)) } returns mapOf(flag.key to flag)
            coEvery { cohortLoader.loadCohorts(allAny()) } returns Unit
            val deploymentRunner =
                DeploymentRunner(
                    configuration,
                    deploymentKey,
                    cohortLoader,
                    deploymentStorage,
                    deploymentLoader,
                )
            deploymentRunner.start()
            delay(75)
            deploymentRunner.stop()
            coVerify(exactly = 2) { deploymentLoader.loadDeployment(eq(deploymentKey)) }
            coVerify(exactly = 1) { deploymentStorage.getAllFlags(eq(deploymentKey)) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
        }

    @Test
    fun `start, load deployment throws, does not throw, pollers run`(): Unit =
        runBlocking {
            val cohortIds = setOf("a")
            val flag = flag(cohortIds = cohortIds)
            val configuration =
                Configuration(
                    flagSyncIntervalMillis = 50,
                    cohortSyncIntervalMillis = 50,
                )
            val cohortLoader = mockk<CohortLoader>()
            val deploymentStorage = mockk<DeploymentStorage>()
            val deploymentLoader = mockk<DeploymentLoader>()
            coEvery { deploymentLoader.loadDeployment(eq(deploymentKey)) } throws RuntimeException("fail")
            coEvery { deploymentStorage.getAllFlags(eq(deploymentKey)) } returns mapOf(flag.key to flag)
            coEvery { cohortLoader.loadCohorts(allAny()) } returns Unit
            val deploymentRunner =
                DeploymentRunner(
                    configuration,
                    deploymentKey,
                    cohortLoader,
                    deploymentStorage,
                    deploymentLoader,
                )
            deploymentRunner.start()
            delay(75)
            deploymentRunner.stop()
            coVerify(exactly = 2) { deploymentLoader.loadDeployment(eq(deploymentKey)) }
            coVerify(exactly = 1) { deploymentStorage.getAllFlags(eq(deploymentKey)) }
            coVerify(exactly = 1) { cohortLoader.loadCohorts(eq(cohortIds)) }
        }

    @Test
    fun `start get all flags throws, does not throw`(): Unit =
        runBlocking {
            val configuration =
                Configuration(
                    flagSyncIntervalMillis = 10,
                    cohortSyncIntervalMillis = 10,
                )
            val cohortLoader = mockk<CohortLoader>()
            val deploymentStorage = mockk<DeploymentStorage>()
            val deploymentLoader = mockk<DeploymentLoader>()
            coEvery { deploymentLoader.loadDeployment(eq(deploymentKey)) } returns Unit
            coEvery { deploymentStorage.getAllFlags(eq(deploymentKey)) } throws RuntimeException("fail")
            coEvery { cohortLoader.loadCohorts(allAny()) } returns Unit
            val deploymentRunner =
                DeploymentRunner(
                    configuration,
                    deploymentKey,
                    cohortLoader,
                    deploymentStorage,
                    deploymentLoader,
                )
            deploymentRunner.start()
            delay(100)
            deploymentRunner.stop()
            coVerify(atLeast = 3) { deploymentLoader.loadDeployment(eq(deploymentKey)) }
            coVerify(atLeast = 2) { deploymentStorage.getAllFlags(eq(deploymentKey)) }
            coVerify(exactly = 0) { cohortLoader.loadCohorts(allAny()) }
        }
}
