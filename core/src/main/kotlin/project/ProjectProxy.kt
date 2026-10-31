package com.amplitude.project

import com.amplitude.Configuration
import com.amplitude.EvaluationProxyResponse
import com.amplitude.Metrics
import com.amplitude.RemoteEvaluationFlagSkipped
import com.amplitude.assignment.Assignment
import com.amplitude.assignment.AssignmentTracker
import com.amplitude.cohort.CohortApiV1
import com.amplitude.cohort.CohortLoader
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.USER_GROUP_TYPE
import com.amplitude.deployment.DeploymentApiV2
import com.amplitude.deployment.DeploymentLoader
import com.amplitude.deployment.DeploymentStorage
import com.amplitude.experiment.evaluation.EvaluationEngineImpl
import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.experiment.evaluation.topologicalSort
import com.amplitude.exposure.Exposure
import com.amplitude.exposure.ExposureTracker
import com.amplitude.util.isRemoteEvaluationMode
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.toEvaluationContext
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.encodeToString
import java.util.concurrent.ConcurrentHashMap

internal class ProjectProxy(
    private val project: Project,
    configuration: Configuration,
    private val assignmentTracker: AssignmentTracker,
    private val exposureTracker: ExposureTracker,
    private val deploymentStorage: DeploymentStorage,
    private val cohortStorage: CohortStorage,
) {
    companion object {
        val log by logger()
    }

    private val engine = EvaluationEngineImpl()

    // Remote evaluation mode flags we have already warned about, so the warning is logged once per flag.
    private val warnedRemoteFlagKeys: MutableSet<String> = ConcurrentHashMap.newKeySet()

    private val projectApi = ProjectApiV1(configuration.managementServerUrl, project.managementKey)
    private val deploymentApi = DeploymentApiV2(configuration.serverUrl)
    private val cohortApi = CohortApiV1(configuration.cohortServerUrl, project.apiKey, project.secretKey)
    private val cohortLoader = CohortLoader(configuration.maxCohortSize, cohortApi, cohortStorage)
    private val deploymentLoader = DeploymentLoader(deploymentApi, deploymentStorage, cohortLoader)
    private val projectRunner =
        ProjectRunner(
            project,
            configuration,
            projectApi,
            deploymentLoader,
            deploymentStorage,
            cohortLoader,
            cohortStorage,
        )

    suspend fun start() {
        log.info("Starting project. projectId=${project.id}")
        projectRunner.start()
    }

    suspend fun shutdown() {
        log.info("Shutting down project. project.id=${project.id}")
        projectRunner.stop()
    }

    suspend fun getFlagConfigs(deploymentKey: String?): EvaluationProxyResponse {
        if (deploymentKey.isNullOrEmpty()) {
            return EvaluationProxyResponse.error(HttpStatusCode.Unauthorized, "Invalid deployment")
        }
        val result = deploymentStorage.getAllFlags(deploymentKey).values.toList()
        return EvaluationProxyResponse.error(HttpStatusCode.OK, json.encodeToString(result))
    }

    suspend fun getCohort(
        cohortId: String?,
        lastModified: Long?,
        maxCohortSize: Int?,
    ): EvaluationProxyResponse {
        if (cohortId.isNullOrEmpty()) {
            return EvaluationProxyResponse.error(HttpStatusCode.NotFound, "Cohort not found")
        }
        val cohortDescription =
            cohortStorage.getCohortDescription(cohortId)
                ?: return EvaluationProxyResponse.error(HttpStatusCode.NotFound, "Cohort not found")
        if (cohortDescription.size > (maxCohortSize ?: Int.MAX_VALUE)) {
            return EvaluationProxyResponse.error(
                HttpStatusCode.PayloadTooLarge,
                "Cohort $cohortId sized ${cohortDescription.size} is greater than max cohort size $maxCohortSize",
            )
        }
        if (cohortDescription.lastModified == lastModified) {
            return EvaluationProxyResponse.error(HttpStatusCode.NoContent, "Cohort not modified")
        }
        cohortStorage.getCohortBlob(cohortId)?.let { gz ->
            return EvaluationProxyResponse.bytes(
                status = HttpStatusCode.OK,
                payload = gz,
            )
        }
        return EvaluationProxyResponse.error(HttpStatusCode.NotFound, "Cohort members not found")
    }

    suspend fun getCohortMemberships(
        deploymentKey: String?,
        groupType: String?,
        groupName: String?,
    ): EvaluationProxyResponse {
        if (deploymentKey.isNullOrEmpty()) {
            return EvaluationProxyResponse.error(HttpStatusCode.Unauthorized, "Invalid deployment")
        }
        if (groupType.isNullOrEmpty()) {
            return EvaluationProxyResponse.error(HttpStatusCode.BadRequest, "Invalid group type")
        }
        if (groupName.isNullOrEmpty()) {
            return EvaluationProxyResponse.error(HttpStatusCode.BadRequest, "Invalid group name")
        }
        val result = cohortStorage.getCohortMemberships(groupType, groupName)
        return EvaluationProxyResponse.json(HttpStatusCode.OK, result)
    }

    suspend fun evaluate(
        deploymentKey: String?,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null,
        tracksExposure: Boolean = false,
        tracksAssignment: Boolean = true,
    ): EvaluationProxyResponse {
        if (deploymentKey.isNullOrEmpty()) {
            return EvaluationProxyResponse.error(HttpStatusCode.Unauthorized, "Invalid deployment")
        }
        val result = evaluateInternal(deploymentKey, user, flagKeys, tracksExposure, tracksAssignment)
        return EvaluationProxyResponse(HttpStatusCode.OK, json.encodeToString(result))
    }

    suspend fun evaluateV1(
        deploymentKey: String?,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null,
        tracksExposure: Boolean = false,
        tracksAssignment: Boolean = true,
    ): EvaluationProxyResponse {
        if (deploymentKey.isNullOrEmpty()) {
            return EvaluationProxyResponse(HttpStatusCode.Unauthorized, "Invalid deployment")
        }
        val result =
            evaluateInternal(deploymentKey, user, flagKeys, tracksExposure, tracksAssignment).filter { entry ->
                val default = entry.value.metadata?.get("default") as? Boolean ?: false
                val deployed = entry.value.metadata?.get("deployed") as? Boolean ?: true
                (!default && deployed)
            }
        return EvaluationProxyResponse(HttpStatusCode.OK, json.encodeToString(result))
    }

    private suspend fun evaluateInternal(
        deploymentKey: String,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null,
        tracksExposure: Boolean = false,
        tracksAssignment: Boolean = true,
    ): Map<String, EvaluationVariant> {
        // Get flag configs for the deployment from storage and topo sort.
        val storageFlags = deploymentStorage.getAllFlags(deploymentKey)
        if (storageFlags.isEmpty()) {
            return mapOf()
        }
        val flags = topologicalSort(storageFlags, flagKeys ?: setOf())
        if (flags.isEmpty()) {
            return mapOf()
        }
        // Flags in remote evaluation mode cannot be evaluated by the proxy: the
        // flag server only returns a default stub for them (no targeting rules).
        // They are kept for dependency resolution but omitted from the result,
        // rather than answering with the stub's default as if it were a decision.
        val remoteFlagKeys = flags.filter { it.isRemoteEvaluationMode() }.map { it.key }.toSet()
        for (remoteFlagKey in remoteFlagKeys) {
            Metrics.track(RemoteEvaluationFlagSkipped)
            if (warnedRemoteFlagKeys.add(remoteFlagKey)) {
                log.warn(
                    "Flag {} is configured for remote evaluation and cannot be evaluated by the " +
                        "evaluation proxy; it is omitted from local evaluation results. projectId={}",
                    remoteFlagKey,
                    project.id,
                )
            }
        }
        // Enrich user with cohort IDs and build the evaluation context
        val userId = user?.get("user_id") as? String
        val enrichedUser = user?.toMutableMap() ?: mutableMapOf()
        if (userId != null) {
            enrichedUser["cohort_ids"] = cohortStorage.getCohortMemberships(USER_GROUP_TYPE, userId)
        }
        val groups = enrichedUser["groups"] as? Map<*, *>
        if (!groups.isNullOrEmpty()) {
            val groupCohortIds = mutableMapOf<String, Map<String, Set<String>>>()
            for (entry in groups.entries) {
                val groupType = entry.key as? String
                val groupName = (entry.value as? Collection<*>)?.firstOrNull() as? String
                if (groupType != null && groupName != null) {
                    val cohortIds = cohortStorage.getCohortMemberships(groupType, groupName)
                    if (groupCohortIds.isNotEmpty()) {
                        groupCohortIds.putIfAbsent(groupType, mutableMapOf(groupName to cohortIds))
                    }
                }
            }
            if (groupCohortIds.isNotEmpty()) {
                enrichedUser["group_cohort_ids"] = groupCohortIds
            }
        }
        val evaluationContext = enrichedUser.toEvaluationContext()
        // Evaluate results
        log.debug("evaluate - context={}", evaluationContext)
        val result =
            engine.evaluate(evaluationContext, flags).let { evaluated ->
                if (remoteFlagKeys.isEmpty()) evaluated else evaluated.filterKeys { it !in remoteFlagKeys }
            }
        if (enrichedUser.isNotEmpty() && result.isNotEmpty()) {
            coroutineScope {
                if (tracksAssignment) {
                    launch {
                        assignmentTracker.track(Assignment(evaluationContext, result))
                    }
                }
                if (tracksExposure) {
                    launch {
                        exposureTracker.track(Exposure(evaluationContext, result))
                    }
                }
            }
        }
        return result
    }

    // Internal

    internal suspend fun getDeployments(): Set<String> {
        return deploymentStorage.getDeployments().keys
    }
}
