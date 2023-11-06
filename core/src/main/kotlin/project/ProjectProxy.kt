package com.amplitude.project

import com.amplitude.Configuration
import com.amplitude.HttpErrorResponseException
import com.amplitude.assignment.Assignment
import com.amplitude.assignment.AssignmentTracker
import com.amplitude.cohort.CohortApiV5
import com.amplitude.cohort.CohortDescription
import com.amplitude.cohort.CohortStorage
import com.amplitude.deployment.DeploymentApiV1
import com.amplitude.deployment.DeploymentStorage
import com.amplitude.experiment.evaluation.EvaluationEngineImpl
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.experiment.evaluation.topologicalSort
import com.amplitude.util.getCohortIds
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
        val cohortIds = deploymentStorage.getAllFlags(deploymentKey).values.getCohortIds()
        return cohortStorage.getCohortMembershipsForUser(userId, cohortIds)
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
        val enrichedUser = if (userId != null) {
            user.toMutableMap().apply {
                put("cohort_ids", cohortStorage.getCohortMembershipsForUser(userId))
            }
        } else {
            null
        }
        val evaluationContext = enrichedUser.toEvaluationContext()
        // Evaluate results
        log.debug("evaluate - context={}", evaluationContext)
        val result = engine.evaluate(evaluationContext, flags)
        if (enrichedUser != null) {
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
