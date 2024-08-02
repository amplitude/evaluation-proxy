package project

import test.cohort
import com.amplitude.Configuration
import com.amplitude.cohort.CohortLoader
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.cohort.toCohortDescription
import com.amplitude.deployment.DeploymentLoader
import com.amplitude.deployment.DeploymentStorage
import com.amplitude.deployment.InMemoryDeploymentStorage
import com.amplitude.project.ProjectApi
import com.amplitude.project.ProjectRunner
import test.deployment
import test.flag
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertNull
import test.project
import org.junit.Test
import kotlin.test.assertNotNull

class ProjectRunnerTest {

    private val project = project()
    private val config = Configuration(deploymentSyncIntervalMillis = 50)

    @Test
    fun `test start, no state initial load, success`(): Unit = runBlocking {
        val projectApi = mockk<ProjectApi>()
        val deploymentLoader = mockk<DeploymentLoader>()
        val deploymentStorage = spyk<DeploymentStorage>(InMemoryDeploymentStorage())
        val cohortLoader = mockk<CohortLoader>()
        val cohortStorage = spyk<CohortStorage>(InMemoryCohortStorage())
        val runner = ProjectRunner(
            project,
            config,
            projectApi,
            deploymentLoader,
            deploymentStorage,
            cohortLoader,
            cohortStorage
        )
        coEvery { projectApi.getDeployments() } returns listOf(deployment("a"))
        coEvery { cohortLoader.loadCohorts(allAny()) } returns Unit
        coEvery { deploymentLoader.loadDeployment(allAny()) } returns Unit
        runner.start()
        runner.stop()
        coVerify(exactly = 1) { projectApi.getDeployments() }
        coVerify(exactly = 1) { deploymentStorage.getDeployments() }
        coVerify(exactly = 1) { deploymentStorage.putDeployment(eq(deployment("a"))) }
        assertNotNull(runner.deploymentRunners["a"])
        coVerify(exactly = 0) { deploymentStorage.removeAllFlags(allAny()) }
        coVerify(exactly = 0) { deploymentStorage.removeDeployment(allAny()) }
        coVerify(exactly = 1) { deploymentStorage.getAllFlags(allAny()) }
        coVerify(exactly = 1) { cohortStorage.getCohortDescriptions() }
        coVerify(exactly = 0) { cohortStorage.deleteCohort(allAny()) }
    }

    @Test
    fun `test start, with initial state, add and remove deployment, success`(): Unit = runBlocking {
        val projectApi = mockk<ProjectApi>()
        val deploymentLoader = mockk<DeploymentLoader>()
        val deploymentStorage = spyk<DeploymentStorage>(InMemoryDeploymentStorage().apply {
            putDeployment(deployment("a"))
            putFlag("a", flag(cohortIds = setOf("aa")))
        })
        val cohortLoader = mockk<CohortLoader>()
        val cohortStorage = spyk<CohortStorage>(InMemoryCohortStorage().apply {
            putCohort(cohort("aa"))
        })
        coEvery { projectApi.getDeployments() } returns listOf(deployment("b"))
        coEvery { cohortLoader.loadCohorts(allAny()) } returns Unit
        coEvery { deploymentLoader.loadDeployment(allAny()) } returns Unit
        val runner = ProjectRunner(
            project,
            config,
            projectApi,
            deploymentLoader,
            deploymentStorage,
            cohortLoader,
            cohortStorage
        )
        runner.start()
        runner.stop()
        coVerify(exactly = 1) { projectApi.getDeployments() }
        coVerify(exactly = 1) { deploymentStorage.getDeployments() }
        coVerify(exactly = 1) { deploymentStorage.putDeployment(eq(deployment("b"))) }
        assertNull(runner.deploymentRunners["a"])
        assertNotNull(runner.deploymentRunners["b"])
        coVerify(exactly = 1) { deploymentStorage.removeAllFlags(eq("a")) }
        coVerify(exactly = 1) { deploymentStorage.removeDeployment(eq("a")) }
        coVerify(exactly = 1) { deploymentStorage.getAllFlags(eq("b")) }
        coVerify(exactly = 1) { cohortStorage.getCohortDescriptions() }
        coVerify(exactly = 1) { cohortStorage.deleteCohort(eq(cohort("aa").toCohortDescription())) }
    }

    @Test
    fun `test start, deployments api failure, does not throw`(): Unit = runBlocking {
        val projectApi = mockk<ProjectApi>()
        val deploymentLoader = mockk<DeploymentLoader>()
        val deploymentStorage = spyk<DeploymentStorage>(InMemoryDeploymentStorage())
        val cohortLoader = mockk<CohortLoader>()
        val cohortStorage = spyk<CohortStorage>(InMemoryCohortStorage())
        coEvery { projectApi.getDeployments() } throws RuntimeException("test")
        coEvery { cohortLoader.loadCohorts(allAny()) } returns Unit
        coEvery { deploymentLoader.loadDeployment(allAny()) } returns Unit
        val runner = ProjectRunner(
            project,
            config,
            projectApi,
            deploymentLoader,
            deploymentStorage,
            cohortLoader,
            cohortStorage
        )
        runner.start()
        delay(150)
        // Pollers should start and call getDeployments again
        coVerify(atLeast = 2) { projectApi.getDeployments() }

    }
}
