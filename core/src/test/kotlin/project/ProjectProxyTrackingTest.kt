package project

import com.amplitude.Configuration
import com.amplitude.assignment.Assignment
import com.amplitude.assignment.AssignmentTracker
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.deployment.InMemoryDeploymentStorage
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.experiment.evaluation.EvaluationSegment
import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.exposure.Exposure
import com.amplitude.exposure.ExposureTracker
import com.amplitude.project.ProjectProxy
import io.ktor.http.HttpStatusCode
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import test.deployment
import test.flag
import test.project
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProjectProxyTrackingTest {
    private val project = project()
    private val configuration = Configuration()
    private val deployment = deployment("deployment")
    private val user = mapOf("user_id" to "user-1")

    private fun proxy(
        assignmentTracker: AssignmentTracker,
        exposureTracker: ExposureTracker,
        vararg flags: EvaluationFlag,
    ): ProjectProxy {
        val deploymentStorage =
            InMemoryDeploymentStorage().apply {
                runBlocking { flags.forEach { putFlag(deployment.key, it) } }
            }
        return ProjectProxy(
            project,
            configuration,
            assignmentTracker,
            exposureTracker,
            deploymentStorage,
            InMemoryCohortStorage(),
        )
    }

    private fun keysOf(body: String): Set<String> = Json.parseToJsonElement(body).let { (it as JsonObject).keys }

    @Test
    fun `evaluate tracks assignment by default`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val exposureTracker = mockk<ExposureTracker>()
            coEvery { assignmentTracker.track(any()) } returns Unit
            val projectProxy = proxy(assignmentTracker, exposureTracker, flag("flag"))

            val result = projectProxy.evaluate(deployment.key, user)
            assertEquals(HttpStatusCode.OK, result.status)
            assertEquals(setOf("flag"), keysOf(result.body))
            coVerify(exactly = 1) { assignmentTracker.track(any()) }
        }

    @Test
    fun `evaluate with no-track does not track assignment`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val exposureTracker = mockk<ExposureTracker>()
            val projectProxy = proxy(assignmentTracker, exposureTracker, flag("flag"))

            val result = projectProxy.evaluate(deployment.key, user, null, tracksAssignment = false)
            assertEquals(HttpStatusCode.OK, result.status)
            assertEquals(setOf("flag"), keysOf(result.body))
            coVerify(exactly = 0) { assignmentTracker.track(any()) }
            coVerify(exactly = 0) { exposureTracker.track(any()) }
        }

    @Test
    fun `evaluateV1 with no-track does not track assignment`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val exposureTracker = mockk<ExposureTracker>()
            val projectProxy = proxy(assignmentTracker, exposureTracker, flag("flag"))

            val result = projectProxy.evaluateV1(deployment.key, user, null, tracksAssignment = false)
            assertEquals(HttpStatusCode.OK, result.status)
            coVerify(exactly = 0) { assignmentTracker.track(any()) }
        }

    @Test
    fun `evaluate with no-track still tracks exposure when requested`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val exposureTracker = mockk<ExposureTracker>()
            coEvery { exposureTracker.track(any()) } returns Unit
            val projectProxy = proxy(assignmentTracker, exposureTracker, flag("flag"))

            projectProxy.evaluate(deployment.key, user, null, tracksAssignment = false, tracksExposure = true)
            coVerify(exactly = 0) { assignmentTracker.track(any()) }
            coVerify(exactly = 1) { exposureTracker.track(any()) }
        }

    // Shape of what the flag server returns for a remote evaluation mode flag: variants,
    // a single "everyone -> off" segment and evaluationMode=remote, but no targeting rules.
    private fun remoteStub(key: String) =
        EvaluationFlag(
            key = key,
            variants =
                mapOf(
                    "off" to EvaluationVariant("off", null, null, mapOf("default" to true)),
                    "treatment" to EvaluationVariant("treatment", "treatment", null, null),
                ),
            segments = listOf(EvaluationSegment(variant = "off", metadata = mapOf("segmentName" to "default"))),
            metadata = mapOf("evaluationMode" to "remote", "flagType" to "release", "flagVersion" to 11, "deployed" to true),
        )

    private fun variantKeyOf(
        body: String,
        flagKey: String,
    ): String? = ((Json.parseToJsonElement(body) as JsonObject)[flagKey] as? JsonObject)?.get("key")?.toString()?.trim('"')

    @Test
    fun `remote evaluation mode flag returns its default like the SDKs but is not tracked`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val exposureTracker = mockk<ExposureTracker>()
            val assignmentSlot = slot<Assignment>()
            val exposureSlot = slot<Exposure>()
            coEvery { assignmentTracker.track(capture(assignmentSlot)) } returns Unit
            coEvery { exposureTracker.track(capture(exposureSlot)) } returns Unit
            val localFlag = flag("local", metadata = mapOf("evaluationMode" to "local", "flagVersion" to 1))
            val projectProxy = proxy(assignmentTracker, exposureTracker, localFlag, remoteStub("remote"))

            val result = projectProxy.evaluate(deployment.key, user, null, tracksExposure = true)
            assertEquals(HttpStatusCode.OK, result.status)
            // Response matches local evaluation in the SDKs: the stub's default is returned.
            assertEquals(setOf("local", "remote"), keysOf(result.body))
            assertEquals("off", variantKeyOf(result.body, "remote"))
            // Tracking excludes the remote flag so the proxy cannot overwrite Amplitude's results.
            assertTrue(assignmentSlot.isCaptured)
            assertEquals(setOf("local"), assignmentSlot.captured.results.keys)
            assertTrue(exposureSlot.isCaptured)
            assertEquals(setOf("local"), exposureSlot.captured.results.keys)
        }

    @Test
    fun `remote evaluation mode flag requested alone returns its default and tracks nothing`(): Unit =
        runBlocking {
            val assignmentTracker = mockk<AssignmentTracker>()
            val exposureTracker = mockk<ExposureTracker>()
            val projectProxy = proxy(assignmentTracker, exposureTracker, flag("local"), remoteStub("remote"))

            val result = projectProxy.evaluate(deployment.key, user, setOf("remote"), tracksExposure = true)
            assertEquals(HttpStatusCode.OK, result.status)
            assertEquals(setOf("remote"), keysOf(result.body))
            assertEquals("off", variantKeyOf(result.body, "remote"))
            coVerify(exactly = 0) { assignmentTracker.track(any()) }
            coVerify(exactly = 0) { exposureTracker.track(any()) }
        }

    @Test
    fun `get flag configs still returns remote evaluation mode flags for local SDKs`(): Unit =
        runBlocking {
            val remoteFlag = flag("remote", metadata = mapOf("evaluationMode" to "remote", "flagVersion" to 1))
            val projectProxy = proxy(mockk(), mockk(), flag("local"), remoteFlag)

            val result = projectProxy.getFlagConfigs(deployment.key)
            assertEquals(HttpStatusCode.OK, result.status)
            assertTrue(result.body.contains("\"remote\""), result.body)
            assertFalse(result.body.isEmpty())
        }
}
