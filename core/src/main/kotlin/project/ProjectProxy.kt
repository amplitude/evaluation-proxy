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
import com.amplitude.experiment.evaluation.FlagConfig
import com.amplitude.experiment.evaluation.FlagResult
import com.amplitude.experiment.evaluation.SkylabUser
import com.amplitude.experiment.evaluation.Variant
import com.amplitude.util.getCohortIds
import com.amplitude.util.logger
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class ProjectProxy(
    private val project: Project,
    configuration: Configuration = Configuration(),
) {

    companion object {
        val log by logger()
    }

    private val engine = EvaluationEngineImpl()

    private val assignmentTracker = AmplitudeAssignmentTracker(project.apiKey, configuration.assignment)
    private val deploymentApi = DeploymentApiV1()
    private val deploymentStorage = getDeploymentStorage(project.id, configuration.redis)
    private val cohortApi = CohortApiV5(apiKey = project.apiKey, secretKey = project.secretKey)
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
        for (deploymentKey in project.deploymentKeys) {
            deploymentStorage.putDeployment(deploymentKey)
        }
        projectRunner.start()
    }

    suspend fun shutdown() {
        log.info("Shutting down project. projectId=${project.id}")
        projectRunner.stop()
    }

    suspend fun getFlagConfigs(deploymentKey: String?): List<FlagConfig> {
        if (deploymentKey.isNullOrEmpty() || !deploymentKey.startsWith("server-")) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
        return deploymentStorage.getFlagConfigs(deploymentKey)
            ?: throw HttpErrorResponseException(status = 404, message = "Unknown deployment.")
    }

    suspend fun getCohortMembershipsForUser(deploymentKey: String?, userId: String?): Set<String> {
        if (deploymentKey.isNullOrEmpty() || !deploymentKey.startsWith("server-")) {
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
        user: SkylabUser?,
        flagKeys: Set<String>? = null
    ): Map<String, Variant> {
        if (deploymentKey.isNullOrEmpty() || !deploymentKey.startsWith("server-")) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
        // Get flag configs for the deployment from storage.
        val flagConfigs = deploymentStorage.getFlagConfigs(deploymentKey)
        if (flagConfigs == null || flagConfigs.isEmpty()) {
            return mapOf()
        }
        // Enrich user with cohort IDs.
        val enrichedUser = user?.userId?.let { userId ->
            user.copy(cohortIds = cohortStorage.getCohortMembershipsForUser(userId))
        }
        // Evaluate results
        log.debug("evaluate - user=$enrichedUser")
        val result = engine.evaluate(flagConfigs, enrichedUser)
        if (enrichedUser != null) {
            coroutineScope {
                launch {
                    assignmentTracker.track(Assignment(enrichedUser, result))
                }
            }
        }
        return result.filterDeployedVariants(flagKeys)
    }

    /**
     * Filter only non-default, deployed variants from the results that are included if flag keys (if not empty).
     */
    private fun Map<String, FlagResult>.filterDeployedVariants(flagKeys: Set<String>?): Map<String, Variant> {
        return filter { entry ->
            val isVariant = !entry.value.isDefaultVariant
            val isIncluded = (flagKeys.isNullOrEmpty() || flagKeys.contains(entry.key))
            val isDeployed = entry.value.deployed
            isVariant && isIncluded && isDeployed
        }.mapValues { entry ->
            entry.value.variant
        }.toMap()
    }
}
