package com.amplitude

import com.amplitude.assignment.AmplitudeAssignmentTracker
import com.amplitude.assignment.Assignment
import com.amplitude.cohort.CohortApiV5
import com.amplitude.cohort.InMemoryCohortStorage
import com.amplitude.cohort.RedisCohortStorage
import com.amplitude.deployment.DeploymentApiV1
import com.amplitude.deployment.InMemoryDeploymentStorage
import com.amplitude.deployment.RedisDeploymentStorage
import com.amplitude.experiment.evaluation.EvaluationEngineImpl
import com.amplitude.experiment.evaluation.FlagConfig
import com.amplitude.experiment.evaluation.FlagResult
import com.amplitude.experiment.evaluation.SkylabUser
import com.amplitude.experiment.evaluation.Variant
import com.amplitude.experiment.evaluation.serialization.SerialFlagConfig
import com.amplitude.experiment.evaluation.serialization.SerialVariant
import com.amplitude.project.ProjectRunner
import com.amplitude.util.HttpErrorResponseException
import com.amplitude.util.getCohortIds
import com.amplitude.util.json
import com.amplitude.util.logger
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.encodeToString
import kotlin.time.DurationUnit
import kotlin.time.toDuration

const val VERSION = "0.1.0"

class HttpErrorResponseException(
    val status: Int,
    override val message: String,
    override val cause: Exception? = null,
) : Exception(message, cause)

class EvaluationProxy(
    apiKey: String,
    secretKey: String,
    private val deploymentKeys: Set<String>,
    configuration: Configuration = Configuration(),
) {

    companion object {
        val log by logger()
    }

    private val engine = EvaluationEngineImpl()

    private val assignmentTracker = AmplitudeAssignmentTracker(apiKey, configuration.assignmentConfiguration)
    private val deploymentApi = DeploymentApiV1()
    private val deploymentStorage = if (configuration.redisConfiguration?.redisUrl == null) {
        InMemoryDeploymentStorage()
    } else {
        RedisDeploymentStorage(configuration.redisConfiguration)
    }
    private val cohortApi = CohortApiV5(apiKey = apiKey, secretKey = secretKey)
    private val cohortStorage = if (configuration.redisConfiguration == null) {
        InMemoryCohortStorage()
    } else {
        RedisCohortStorage(
            configuration.redisConfiguration,
            configuration.cohortSyncIntervalMillis.toDuration(DurationUnit.MILLISECONDS)
        )
    }
    private val projectRunner = ProjectRunner(
        configuration,
        deploymentApi,
        deploymentStorage,
        cohortApi,
        cohortStorage
    )

    suspend fun start() {
        for (deploymentKey in deploymentKeys) {
            deploymentStorage.putDeployment(deploymentKey)
        }
        projectRunner.start()
    }

    suspend fun shutdown() {
        projectRunner.stop()
    }

    suspend fun getDeployments(): Set<String> {
        return deploymentStorage.getDeployments()
    }

    suspend fun addDeployment(deploymentKey: String?) {
        if (deploymentKey.isNullOrEmpty() || !deploymentKey.startsWith("server-")) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
        try {
            // HACK: validate deployment by requesting flag configs from evaluation servers.
            deploymentApi.getFlagConfigs(deploymentKey)
        } catch (e: HttpErrorResponseException) {
            when (e.statusCode.value) {
                in 400..499 -> throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
                else -> throw HttpErrorResponseException(status = 503, message = "Unable to validate deployment.")
            }
        }
        deploymentStorage.putDeployment(deploymentKey)
    }

    suspend fun removeDeployment(deploymentKey: String?) {
        if (deploymentKey.isNullOrEmpty() || !deploymentKey.startsWith("server-")) {
            throw HttpErrorResponseException(status = 401, message = "Invalid deployment.")
        }
        deploymentStorage.removeDeployment(deploymentKey)
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

suspend fun EvaluationProxy.getSerializedDeployments(): String =
    getDeployments().encodeToJsonString()

suspend fun EvaluationProxy.getSerializedFlagConfigs(deploymentKey: String?): String =
    getFlagConfigs(deploymentKey).encodeToJsonString()

suspend fun EvaluationProxy.getSerializedCohortMembershipsForUser(deploymentKey: String?, userId: String?): String =
    getCohortMembershipsForUser(deploymentKey, userId).encodeToJsonString()

suspend fun EvaluationProxy.serializedEvaluate(
    deploymentKey: String?,
    user: SkylabUser?,
    flagKeys: Set<String>? = null
) = evaluate(deploymentKey, user, flagKeys).encodeToJsonString()

private fun List<FlagConfig>.encodeToJsonString(): String = json.encodeToString(map { SerialFlagConfig(it) })
private fun Set<String>.encodeToJsonString(): String = json.encodeToString(this)
private fun Map<String, Variant>.encodeToJsonString(): String = json.encodeToString(mapValues { SerialVariant(it.value) })
