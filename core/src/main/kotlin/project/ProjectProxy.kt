package com.amplitude.project

import com.amplitude.Configuration
import com.amplitude.EvaluationProxyResponse
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
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.toEvaluationContext
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.encodeToString

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
        trackExposure: Boolean = false,
    ): EvaluationProxyResponse {
        if (deploymentKey.isNullOrEmpty()) {
            return EvaluationProxyResponse.error(HttpStatusCode.Unauthorized, "Invalid deployment")
        }
        val result = evaluateInternal(deploymentKey, user, flagKeys, trackExposure)
        return EvaluationProxyResponse(HttpStatusCode.OK, json.encodeToString(result))
    }

    suspend fun evaluateV1(
        deploymentKey: String?,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null,
        trackExposure: Boolean = false,
    ): EvaluationProxyResponse {
        if (deploymentKey.isNullOrEmpty()) {
            return EvaluationProxyResponse(HttpStatusCode.Unauthorized, "Invalid deployment")
        }
        val result =
            evaluateInternal(deploymentKey, user, flagKeys, trackExposure).filter { entry ->
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
        trackExposure: Boolean = false,
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
        val result = engine.evaluate(evaluationContext, flags)
        if (enrichedUser.isNotEmpty()) {
            coroutineScope {
                launch {
                    assignmentTracker.track(Assignment(evaluationContext, result))
                }
                if (trackExposure) {
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
