package project

import com.amplitude.Configuration
import com.amplitude.assignment.AssignmentTracker
import com.amplitude.cohort.GetCohortResponse
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.cohort.toCohortDescription
import com.amplitude.deployment.InMemoryDeploymentStorage
import com.amplitude.project.ProjectProxy
import com.amplitude.util.json
import io.ktor.http.HttpStatusCode
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import test.cohort
import test.deployment
import test.flag
import test.project
import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ProjectProxyTest {
    private val project = project()
    private val configuration = Configuration()

    @Test
    fun `test get flag configs, null deployment, unauthorized`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage = InMemoryCohortStorage()
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getFlagConfigs(null)
            assertEquals(HttpStatusCode.Unauthorized, result.status)
        }

    @Test
    fun `test get flag configs, with deployment, success`(): Unit =
        runBlocking {
            val deployment = deployment("deployment")
            val flag = flag("flag")
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage =
                InMemoryDeploymentStorage().apply {
                    putFlag(deployment.key, flag)
                }
            val cohortStorage = InMemoryCohortStorage()
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getFlagConfigs(deployment.key)
            assertEquals(HttpStatusCode.OK, result.status)
            assertEquals(json.encodeToString(listOf(flag)), result.body)
        }

    @Test
    fun `test get cohort, null cohort id, not found`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage = InMemoryCohortStorage()
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohort(null, null, null)
            assertEquals(HttpStatusCode.NotFound, result.status)
        }

    @Test
    fun `test get cohort, with cohort id, success`(): Unit =
        runBlocking {
            val cohort = cohort("a")
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage =
                InMemoryCohortStorage().apply {
                    val acc = createWriter(cohort.toCohortDescription())
                    acc.addMembers(cohort.members.toList())
                    acc.complete(cohort.members.size)
                }
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohort("a", null, null)
            assertEquals(HttpStatusCode.OK, result.status)
            val bytes = result.bytes
            assertNotNull(bytes)
            val jsonStr = ungzipToString(bytes)
            val decoded = json.decodeFromString<GetCohortResponse>(jsonStr)
            assertEquals(GetCohortResponse.fromCohort(cohort), decoded)
        }

    @Test
    fun `test get cohort, with cohort id and last modified, success`(): Unit =
        runBlocking {
            val cohort = cohort("a", 100)
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage =
                InMemoryCohortStorage().apply {
                    val acc = createWriter(cohort.toCohortDescription())
                    acc.addMembers(cohort.members.toList())
                    acc.complete(cohort.members.size)
                }
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohort("a", 1, null)
            assertEquals(HttpStatusCode.OK, result.status)
            val bytes = result.bytes
            assertNotNull(bytes)
            val jsonStr = ungzipToString(bytes)
            val decoded = json.decodeFromString<GetCohortResponse>(jsonStr)
            assertEquals(GetCohortResponse.fromCohort(cohort), decoded)
        }

    @Test
    fun `test get cohort, with cohort id and last modified, not changed`(): Unit =
        runBlocking {
            val cohort = cohort("a", 100)
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage =
                InMemoryCohortStorage().apply {
                    val acc = createWriter(cohort.toCohortDescription())
                    acc.addMembers(cohort.members.toList())
                    acc.complete(cohort.members.size)
                }
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohort("a", 100, null)
            assertEquals(HttpStatusCode.NoContent, result.status)
        }

    @Test
    fun `test get cohort, with cohort id and max cohort size, success`(): Unit =
        runBlocking {
            val deployment = deployment("deployment")
            val flag = flag("flag", setOf("a"))
            val cohort = cohort("a", 100, size = 100)
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage =
                InMemoryDeploymentStorage().apply {
                    putFlag(deployment.key, flag)
                }
            val cohortStorage =
                InMemoryCohortStorage().apply {
                    val acc = createWriter(cohort.toCohortDescription())
                    acc.addMembers(cohort.members.toList())
                    acc.complete(cohort.members.size)
                }
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohort("a", null, Int.MAX_VALUE)
            assertEquals(HttpStatusCode.OK, result.status)
            val bytes = result.bytes
            assertNotNull(bytes)
            val jsonStr = ungzipToString(bytes)
            val decoded = json.decodeFromString<GetCohortResponse>(jsonStr)
            assertEquals(GetCohortResponse.fromCohort(cohort), decoded)
        }

    @Test
    fun `test get cohort, with cohort id and max cohort size, too large`(): Unit =
        runBlocking {
            val cohort = cohort("a", 100, size = 100)
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage =
                InMemoryCohortStorage().apply {
                    val acc = createWriter(cohort.toCohortDescription())
                    acc.addMembers(cohort.members.toList())
                    acc.complete(cohort.members.size)
                }
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohort("a", null, 99)
            assertEquals(HttpStatusCode.PayloadTooLarge, result.status)
        }

    @Test
    fun `test get cohort memberships for group, null deployment, unauthorized`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage = InMemoryCohortStorage()
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohortMemberships(null, null, null)
            assertEquals(HttpStatusCode.Unauthorized, result.status)
        }

    @Test
    fun `test get cohort memberships for group, with deployment and group type, null group name, bad request`(): Unit =
        runBlocking {
            val deployment = deployment("deployment")
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage = InMemoryCohortStorage()
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohortMemberships(deployment.key, "User", null)
            assertEquals(HttpStatusCode.BadRequest, result.status)
        }

    @Test
    fun `test get cohort memberships for group, with deployment and group name, null group type, bad request`(): Unit =
        runBlocking {
            val deployment = deployment("deployment")
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage = InMemoryCohortStorage()
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohortMemberships(deployment.key, null, "1")
            assertEquals(HttpStatusCode.BadRequest, result.status)
        }

    @Test
    fun `test get cohort memberships for group, with deployment group name and group type, success`(): Unit =
        runBlocking {
            val deployment = deployment("deployment")
            val cohort = cohort("a")
            val flag = flag("flag", setOf("a"))
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage =
                InMemoryDeploymentStorage().apply {
                    putFlag(deployment.key, flag)
                }
            val cohortStorage =
                InMemoryCohortStorage().apply {
                    val acc = createWriter(cohort.toCohortDescription())
                    acc.addMembers(cohort.members.toList())
                    acc.complete(cohort.members.size)
                }
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val result = projectProxy.getCohortMemberships(deployment.key, "User", "1")
            assertEquals(HttpStatusCode.OK, result.status)
            assertEquals(json.encodeToString(listOf(cohort.id)), result.body)
        }

    @Test
    fun `test evaluate, null deployment, unauthorized`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage = InMemoryCohortStorage()
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )

            val result = projectProxy.evaluate(null, null, null)
            assertEquals(HttpStatusCode.Unauthorized, result.status)
        }

    @Test
    fun `test evaluate, with deployment, null user, success`(): Unit =
        runBlocking {
            val deployment = deployment("deployment")
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage = InMemoryDeploymentStorage()
            val cohortStorage = InMemoryCohortStorage()
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )

            val result = projectProxy.evaluate(deployment.key, null, null)
            assertEquals(HttpStatusCode.OK, result.status)
            assertEquals("{}", result.body)
        }

    @Test
    fun `test evaluate, with deployment and flag keys, success`(): Unit =
        runBlocking {
            val cohort = cohort("a")
            val flag = flag("flag", setOf("a"))
            val deployment = deployment("deployment")
            val assignmentTracker = mockk<AssignmentTracker>()
            val deploymentStorage =
                InMemoryDeploymentStorage().apply {
                    putFlag(deployment.key, flag)
                }
            val cohortStorage =
                InMemoryCohortStorage().apply {
                    val acc = createWriter(cohort.toCohortDescription())
                    acc.addMembers(cohort.members.toList())
                    acc.complete(cohort.members.size)
                }
            val projectProxy =
                ProjectProxy(
                    project,
                    configuration,
                    assignmentTracker,
                    deploymentStorage,
                    cohortStorage,
                )
            val user = mapOf("user_id" to "1")
            coEvery { assignmentTracker.track(allAny()) } returns Unit
            val result = projectProxy.evaluate(deployment.key, user, setOf("flag"))
            assertEquals(HttpStatusCode.OK, result.status)
            assertEquals("""{"flag":{"key":"on","value":"on"}}""", result.body)
        }
}

// Helper function to ungzip a byte array to a string
private fun ungzipToString(bytes: ByteArray): String {
    GZIPInputStream(ByteArrayInputStream(bytes)).use { gis ->
        return String(gis.readBytes(), Charsets.UTF_8)
    }
}
