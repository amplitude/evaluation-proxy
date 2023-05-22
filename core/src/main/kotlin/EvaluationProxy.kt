package com.amplitude

import com.amplitude.experiment.evaluation.FlagConfig
import com.amplitude.experiment.evaluation.SkylabUser
import com.amplitude.experiment.evaluation.Variant
import com.amplitude.experiment.evaluation.serialization.SerialFlagConfig
import com.amplitude.experiment.evaluation.serialization.SerialVariant
import com.amplitude.project.ProjectProxy
import com.amplitude.project.getProjectStorage
import com.amplitude.util.json
import com.amplitude.util.logger
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.serialization.encodeToString

const val VERSION = "0.2.0"

class HttpErrorResponseException(
    val status: Int,
    override val message: String,
    override val cause: Exception? = null
) : Exception(message, cause)

class EvaluationProxy(
    private val projects: List<Project>,
    private val configuration: Configuration = Configuration()
) {

    companion object {
        val log by logger()
    }

    private val projectProxies = projects.associateWith { ProjectProxy(it, configuration) }
    private val deploymentProxies = projects
        .map { project -> project.deploymentKeys.associateWith { project }.toMutableMap() }
        .reduce { acc, map -> acc.apply { putAll(map) } }

    private val projectStorage = getProjectStorage(configuration.redis)

    suspend fun start() = coroutineScope {
        log.info("Starting evaluation proxy. projects=${projectProxies.keys.map { it.id }}")
        for (project in projects) {
            projectStorage.putProject(project.id)
        }
        // TODO clean up projects that have been removed.
        projectProxies.map { launch { it.value.start() } }.joinAll()
        log.info("Evaluation proxy started.")
    }

    suspend fun shutdown() = coroutineScope {
        log.info("Shutting down evaluation proxy.")
        projectProxies.map { launch { it.value.shutdown() } }.joinAll()
        log.info("Evaluation proxy shut down.")
    }

    suspend fun getFlagConfigs(deploymentKey: String?): List<FlagConfig> {
        val project = deploymentProxies[deploymentKey] ?: throw HttpErrorResponseException(404, "Deployment not found.")
        val projectProxy = projectProxies[project] ?: throw HttpErrorResponseException(404, "Project not found.")
        return projectProxy.getFlagConfigs(deploymentKey)
    }

    suspend fun getCohortMembershipsForUser(deploymentKey: String?, userId: String?): Set<String> {
        val project = deploymentProxies[deploymentKey] ?: throw HttpErrorResponseException(404, "Deployment not found.")
        val projectProxy = projectProxies[project] ?: throw HttpErrorResponseException(404, "Project not found.")
        return projectProxy.getCohortMembershipsForUser(deploymentKey, userId)
    }

    suspend fun evaluate(
        deploymentKey: String?,
        user: SkylabUser?,
        flagKeys: Set<String>? = null
    ): Map<String, Variant> {
        val project = deploymentProxies[deploymentKey] ?: throw HttpErrorResponseException(404, "Deployment not found.")
        val projectProxy = projectProxies[project] ?: throw HttpErrorResponseException(404, "Project not found.")
        return projectProxy.evaluate(deploymentKey, user, flagKeys)
    }
}

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
private fun Map<String, Variant>.encodeToJsonString(): String =
    json.encodeToString(mapValues { SerialVariant(it.value) })
