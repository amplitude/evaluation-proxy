package com.amplitude.project

import com.amplitude.Configuration
import com.amplitude.HttpErrorResponseException
import com.amplitude.assignment.Assignment
import com.amplitude.assignment.AssignmentTracker
import com.amplitude.cohort.CohortApiV5
import com.amplitude.cohort.CohortDescription
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.USER_GROUP_TYPE
import com.amplitude.deployment.DeploymentApiV1
import com.amplitude.deployment.DeploymentStorage
import com.amplitude.experiment.evaluation.EvaluationEngineImpl
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.experiment.evaluation.topologicalSort
import com.amplitude.util.getGroupedCohortIds
import com.amplitude.util.logger
import com.amplitude.util.toEvaluationContext
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

internal class ProjectProxy(
    private val project: Project,
    configuration: Configuration,
    private val assignmentTracker: AssignmentTracker,
    private val deploymentStorage: DeploymentStorage,
    private val cohortStorage: CohortStorage
) {

    companion object {
        val log by logger()
    }

    private val engine = EvaluationEngineImpl()

    private val projectApi = ProjectApiV1(project.managementKey)
    private val deploymentApi = DeploymentApiV1(configuration.serverUrl)
    private val cohortApi = CohortApiV5(configuration.cohortServerUrl, project.apiKey, project.secretKey)
    private val projectRunner = ProjectRunner(
        configuration,
        projectApi,
        deploymentApi,
        deploymentStorage,
        cohortApi,
        cohortStorage
    )

    suspend fun start() {
        log.info("Starting project. projectId=${project.id}")
        projectRunner.start()
    }

    suspend fun shutdown() {
        log.info("Shutting down project. project.id=${project.id}")
        projectRunner.stop()
    }

    suspend fun getFlagConfigs(deploymentKey: String?): List<EvaluationFlag> {
        if (deploymentKey.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
        return deploymentStorage.getAllFlags(deploymentKey).values.toList()
    }

    suspend fun getCohortDescription(cohortId: String?): CohortDescription {
        if (cohortId.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 404, message = "Cohort not found.")
        }
        return cohortStorage.getCohortDescription(cohortId)
            ?: throw HttpErrorResponseException(status = 404, message = "Cohort not found.")
    }

    suspend fun getCohortMembers(cohortId: String?): Set<String> {
        if (cohortId.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 404, message = "Cohort not found.")
        }
        val cohortDescription = cohortStorage.getCohortDescription(cohortId)
            ?: throw HttpErrorResponseException(status = 404, message = "Cohort not found.")
        return cohortStorage.getCohortMembers(cohortDescription)
            ?: throw HttpErrorResponseException(status = 404, message = "Cohort not found.")
    }

    suspend fun getCohortMembershipsForUser(deploymentKey: String?, userId: String?): Set<String> {
        if (deploymentKey.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
        if (userId.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 400, message = "Invalid user ID.")
        }
        val cohortIds = deploymentStorage.getAllFlags(deploymentKey).values.getGroupedCohortIds()[USER_GROUP_TYPE]
        if (cohortIds.isNullOrEmpty()) {
            return setOf()
        }
        return cohortStorage.getCohortMembershipsForUser(userId, cohortIds)
    }

    suspend fun getCohortMembershipsForGroup(deploymentKey: String?, groupType: String?, groupName: String?): Set<String> {
        if (deploymentKey.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
        if (groupType.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 400, message = "Invalid group type.")
        }
        if (groupName.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 400, message = "Invalid group name.")
        }
        val cohortIds = deploymentStorage.getAllFlags(deploymentKey).values.getGroupedCohortIds()[groupType]
        if (cohortIds.isNullOrEmpty()) {
            return setOf()
        }
        return cohortStorage.getCohortMembershipsForGroup(groupType, groupName, cohortIds)
    }

    suspend fun evaluate(
        deploymentKey: String?,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null
    ): Map<String, EvaluationVariant> {
        if (deploymentKey.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
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
            enrichedUser["cohort_ids"] = cohortStorage.getCohortMembershipsForUser(userId)
        }
        val groups = enrichedUser["groups"] as? Map<*, *>
        if (!groups.isNullOrEmpty()) {
            val groupCohortIds = mutableMapOf<String, Map<String, Set<String>>>()
            for (entry in groups.entries) {
                val groupType = entry.key as? String
                val groupName = (entry.value as? Collection<*>)?.firstOrNull() as? String
                if (groupType != null && groupName != null) {
                    val cohortIds = cohortStorage.getCohortMembershipsForGroup(groupType, groupName)
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
            }
        }
        return result
    }

    suspend fun evaluateV1(
        deploymentKey: String?,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null
    ): Map<String, EvaluationVariant> {
        return evaluate(deploymentKey, user, flagKeys).filter { entry ->
            val default = entry.value.metadata?.get("default") as? Boolean ?: false
            val deployed = entry.value.metadata?.get("deployed") as? Boolean ?: true
            (!default && deployed)
        }
    }

    // Internal

    internal suspend fun getDeployments(): Set<String> {
        return deploymentStorage.getDeployments().keys
    }
}
