package com.amplitude.project

import com.amplitude.Configuration
import com.amplitude.HttpErrorResponseException
import com.amplitude.Project
import com.amplitude.assignment.AmplitudeAssignmentTracker
import com.amplitude.assignment.Assignment
import com.amplitude.cohort.CohortApiV5
import com.amplitude.cohort.getCohortStorage
import com.amplitude.deployment.DeploymentApiV1
import com.amplitude.deployment.getDeploymentStorage
import com.amplitude.experiment.evaluation.EvaluationEngineImpl
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.experiment.evaluation.topologicalSort
import com.amplitude.util.getCohortIds
import com.amplitude.util.logger
import com.amplitude.util.toEvaluationContext
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class ProjectProxy(
    private val project: Project,
    configuration: Configuration = Configuration()
) {

    companion object {
        val log by logger()
    }

    private val engine = EvaluationEngineImpl()

    private val assignmentTracker = AmplitudeAssignmentTracker(project.apiKey, configuration.assignment)
    private val deploymentApi = DeploymentApiV1(configuration.serverUrl)
    private val deploymentStorage = getDeploymentStorage(project.id, configuration.redis)
    private val cohortApi = CohortApiV5(configuration.cohortServerUrl, project.apiKey, project.secretKey)
    private val cohortStorage = getCohortStorage(
        project.id,
        configuration.redis,
        configuration.cohortSyncIntervalMillis.toDuration(DurationUnit.MILLISECONDS)
    )
    private val projectRunner = ProjectRunner(
        configuration,
        deploymentApi,
        deploymentStorage,
        cohortApi,
        cohortStorage
    )

    suspend fun start() {
        log.info("Starting project. projectId=${project.id} deploymentKeys=${project.deploymentKeys}")
        // Add deployments to storage
        for (deploymentKey in project.deploymentKeys) {
            deploymentStorage.putDeployment(deploymentKey)
        }
        // Remove deployments which are no longer being managed
        val storageDeploymentKeys = deploymentStorage.getDeployments()
        for (storageDeploymentKey in storageDeploymentKeys - project.deploymentKeys) {
            deploymentStorage.removeDeployment(storageDeploymentKey)
            deploymentStorage.removeFlagConfigs(storageDeploymentKey)
        }
        projectRunner.start()
    }

    suspend fun shutdown() {
        log.info("Shutting down project. projectId=${project.id}")
        projectRunner.stop()
    }

    suspend fun getFlagConfigs(deploymentKey: String?): List<EvaluationFlag> {
        if (deploymentKey.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
        return deploymentStorage.getFlagConfigs(deploymentKey)
            ?: throw HttpErrorResponseException(status = 404, message = "Unknown deployment.")
    }

    suspend fun getCohortMembershipsForUser(deploymentKey: String?, userId: String?): Set<String> {
        if (deploymentKey.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
        if (userId.isNullOrEmpty()) {
            throw HttpErrorResponseException(status = 400, message = "Invalid user ID.")
        }
        val cohortIds = deploymentStorage.getFlagConfigs(deploymentKey)?.getCohortIds()
            ?: throw HttpErrorResponseException(status = 404, message = "Unknown deployment.")
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
        val storageFlags = deploymentStorage.getFlagConfigs(deploymentKey)
        if (storageFlags.isNullOrEmpty()) {
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
        } else null
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
}
