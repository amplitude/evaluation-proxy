import com.amplitude.Configuration
import com.amplitude.EvaluationProxy
import com.amplitude.ProjectConfiguration
import com.amplitude.project.InMemoryProjectStorage
import com.amplitude.project.ProjectApi
import com.amplitude.project.ProjectProxy
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import test.deployment
import test.project
import java.io.IOException
import kotlin.test.Test

/**
 * Regression tests: a Management API failure for one project must not take the process
 * down, and must not trigger deletion of other projects' data from shared storage.
 */
class EvaluationProxyStartupTest {
    @Test
    fun `start, one project api fails, other project starts and storage cleanup skipped`(): Unit =
        runBlocking {
            val projectStorage =
                spyk(
                    InMemoryProjectStorage().apply {
                        putProject("stale")
                    },
                )
            val project = project("1")
            val projectConfigurations =
                listOf(
                    ProjectConfiguration("api-fail", "secret-fail", "management-fail"),
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
            coEvery { evaluationProxy.createProjectApi(eq("management-fail")) } returns
                mockk<ProjectApi>().apply {
                    coEvery { getDeployments() } throws IOException("management api down")
                }
            coEvery { evaluationProxy.createProjectApi(eq("management")) } returns
                mockk<ProjectApi>().apply {
                    coEvery { getDeployments() } returns listOf(deployment("a", project.id))
                }
            coEvery { evaluationProxy.createProjectProxy(allAny()) } returns
                mockk<ProjectProxy>().apply {
                    coEvery { start() } returns Unit
                }

            // Must not throw.
            evaluationProxy.start()

            assertEquals(1, evaluationProxy.projectProxies.size)
            val projectProxy = evaluationProxy.projectProxies[project]
            coVerify(exactly = 1) { projectProxy?.start() }
            // Stale project must NOT have been removed: we could not load every project.
            coVerify(exactly = 0) { projectStorage.removeProject(any()) }
            coVerify(exactly = 0) { evaluationProxy.createDeploymentStorage(any()) }
            assertTrue(projectStorage.getProjects().contains("stale"))
        }

    @Test
    fun `start, project api returns no deployments, storage cleanup skipped`(): Unit =
        runBlocking {
            val projectStorage =
                spyk(
                    InMemoryProjectStorage().apply {
                        putProject("stale")
                    },
                )
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
                    coEvery { getDeployments() } returns emptyList()
                }

            evaluationProxy.start()

            assertEquals(0, evaluationProxy.projectProxies.size)
            coVerify(exactly = 0) { projectStorage.removeProject(any()) }
            assertTrue(projectStorage.getProjects().contains("stale"))
        }
}
