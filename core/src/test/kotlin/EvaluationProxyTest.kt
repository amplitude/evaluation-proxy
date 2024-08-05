import com.amplitude.Configuration
import com.amplitude.EvaluationProxy
import com.amplitude.ProjectConfiguration
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.toCohortDescription
import com.amplitude.deployment.DeploymentStorage
import com.amplitude.project.InMemoryProjectStorage
import com.amplitude.project.ProjectApi
import com.amplitude.project.ProjectProxy
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import test.cohort
import test.deployment
import test.project
import kotlin.test.Test

class EvaluationProxyTest {
    @Test
    fun `test start, no projects, nothing happens`(): Unit =
        runBlocking {
            val projectStorage = spyk(InMemoryProjectStorage())
            val projectConfigurations = listOf<ProjectConfiguration>()
            val evaluationProxy =
                spyk(
                    EvaluationProxy(
                        projectConfigurations = projectConfigurations,
                        configuration = Configuration(),
                        projectStorage = projectStorage,
                    ),
                )
            evaluationProxy.start()
            verify(exactly = 0) { evaluationProxy.createProjectApi(allAny()) }
            assertEquals(0, evaluationProxy.projectProxies.size)
            coVerify(exactly = 0) { projectStorage.putProject(allAny()) }
        }

    @Test
    fun `test start, with project, storage loaded and proxy started`(): Unit =
        runBlocking {
            val projectStorage = spyk(InMemoryProjectStorage())
            val project = project("1")
            val projectConfigurations =
                listOf(
                    ProjectConfiguration("api", "secret", "management"),
                )
            val evaluationProxy =
                spyk(
                    EvaluationProxy(
                        projectConfigurations = projectConfigurations,
                        configuration = Configuration(),
                        projectStorage = projectStorage,
                    ),
                )
            coEvery { evaluationProxy.createProjectApi(allAny()) } returns
                mockk<ProjectApi>().apply {
                    coEvery { getDeployments() } returns listOf(deployment("a", project.id))
                }
            coEvery { evaluationProxy.createProjectProxy(allAny()) } returns
                mockk<ProjectProxy>().apply {
                    coEvery { start() } returns Unit
                }
            evaluationProxy.start()
            verify(exactly = 1) { evaluationProxy.createProjectApi(allAny()) }
            assertEquals(1, evaluationProxy.projectProxies.size)
            coVerify(exactly = 1) { projectStorage.putProject(allAny()) }
            val projectProxy = evaluationProxy.projectProxies[project]
            coVerify(exactly = 1) { projectProxy?.start() }
        }

    @Test
    fun `test start, stored but no longer configured project deleted`(): Unit =
        runBlocking {
            val projectStorage =
                spyk(
                    InMemoryProjectStorage().apply {
                        putProject("2")
                    },
                )
            val project = project("1")
            val projectConfigurations =
                listOf(
                    ProjectConfiguration("api", "secret", "management"),
                )
            val evaluationProxy =
                spyk(
                    EvaluationProxy(
                        projectConfigurations = projectConfigurations,
                        configuration = Configuration(),
                        projectStorage = projectStorage,
                    ),
                )
            coEvery { evaluationProxy.createProjectApi(allAny()) } returns
                mockk<ProjectApi>().apply {
                    coEvery { getDeployments() } returns listOf(deployment("a", project.id))
                }
            coEvery { evaluationProxy.createProjectProxy(allAny()) } returns
                mockk<ProjectProxy>().apply {
                    coEvery { start() } returns Unit
                }
            val deploymentStorage =
                mockk<DeploymentStorage>().apply {
                    coEvery { getDeployments() } returns mapOf("b" to deployment("b"))
                    coEvery { removeDeployment(eq("b")) } returns Unit
                    coEvery { removeAllFlags(eq("b")) } returns Unit
                }
            val cohortStorage =
                mockk<CohortStorage>().apply {
                    coEvery { getCohortDescriptions() } returns mapOf("c" to cohort("c").toCohortDescription())
                    coEvery { deleteCohort(eq(cohort("c").toCohortDescription())) } returns Unit
                }
            coEvery { evaluationProxy.createDeploymentStorage(allAny()) } returns deploymentStorage
            coEvery { evaluationProxy.createCohortStorage(allAny()) } returns cohortStorage
            evaluationProxy.start()
            verify(exactly = 1) { evaluationProxy.createProjectApi(allAny()) }
            assertEquals(1, evaluationProxy.projectProxies.size)
            coVerify(exactly = 1) { projectStorage.putProject(allAny()) }
            val projectProxy = evaluationProxy.projectProxies[project]
            coVerify(exactly = 1) { projectProxy?.start() }
            // Verify project "2" removed
            coVerify(exactly = 1) { deploymentStorage.getDeployments() }
            coVerify(exactly = 1) { deploymentStorage.removeDeployment(eq("b")) }
            coVerify(exactly = 1) { deploymentStorage.removeAllFlags(eq("b")) }
            coVerify(exactly = 1) { cohortStorage.getCohortDescriptions() }
            coVerify(exactly = 1) { cohortStorage.deleteCohort(eq(cohort("c").toCohortDescription())) }
            coVerify(exactly = 1) { projectStorage.removeProject(eq("2")) }
        }
}
