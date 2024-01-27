package com.amplitude

import com.amplitude.assignment.AmplitudeAssignmentTracker
import com.amplitude.cohort.CohortDescription
import com.amplitude.cohort.getCohortStorage
import com.amplitude.deployment.getDeploymentStorage
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.project.Project
import com.amplitude.project.ProjectApiV1
import com.amplitude.project.ProjectProxy
import com.amplitude.project.getProjectStorage
import com.amplitude.util.json
import com.amplitude.util.logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString
import kotlin.time.DurationUnit
import kotlin.time.toDuration

const val VERSION = "0.3.2"

class HttpErrorResponseException(
    val status: Int,
    override val message: String,
    override val cause: Exception? = null
) : Exception(message, cause)

class EvaluationProxy(
    private val projectConfigurations: List<ProjectConfiguration>,
    private val configuration: Configuration = Configuration(),
    metricsHandler: MetricsHandler? = null
) {

    companion object {
        val log by logger()
    }

    init {
        Metrics.handler = metricsHandler
    }

    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor)

    private val projectProxies = mutableMapOf<Project, ProjectProxy>()
    private val apiKeysToProject = mutableMapOf<String, Project>()
    private val secretKeysToProject = mutableMapOf<String, Project>()
    private val deploymentKeysToProject = mutableMapOf<String, Project>()
    private val mutex = Mutex()

    private val projectStorage = getProjectStorage(configuration.redis)

    suspend fun start() {
        log.info("Starting evaluation proxy.")
        /*
         * Fetch deployments, setup initial mappings for each project
         * configuration, and create the project proxy.
         */
        log.info("Setting up ${projectConfigurations.size} project(s)")
        for (projectConfiguration in projectConfigurations) {
            val projectApi = ProjectApiV1(projectConfiguration.managementKey)
            val deployments = projectApi.getDeployments()
            if (deployments.isEmpty()) {
                continue
            }
            val projectId = deployments.first().projectId
            log.info("Fetched ${deployments.size} deployments for project $projectId")
            // Add the project to local mappings.
            val project = Project(
                id = projectId,
                apiKey = projectConfiguration.apiKey,
                secretKey = projectConfiguration.secretKey,
                managementKey = projectConfiguration.managementKey
            )
            apiKeysToProject[project.apiKey] = project
            secretKeysToProject[project.secretKey] = project
            for (deployment in deployments) {
                deploymentKeysToProject[deployment.key] = project
            }

            // Create a project proxy and add the project to storage.
            val assignmentTracker = AmplitudeAssignmentTracker(project.apiKey, configuration.assignment)
            val deploymentStorage = getDeploymentStorage(project.id, configuration.redis)
            val cohortStorage = getCohortStorage(
                project.id,
                configuration.redis,
                configuration.cohortSyncIntervalMillis.toDuration(DurationUnit.MILLISECONDS)
            )
            val projectProxy = ProjectProxy(
                project,
                configuration,
                assignmentTracker,
                deploymentStorage,
                cohortStorage
            )
            projectProxies[project] = projectProxy
        }

        /*
         * Update project storage with configured projects, and clean up
         * projects that have been removed.
         */
        // Add all configured projects to storage
        val projectIds = projectProxies.map { it.key.id }.toSet()
        for (projectId in projectIds) {
            log.debug("Adding project $projectId")
            projectStorage.putProject(projectId)
        }
        // Remove all non-configured projects and associated data
        val storageProjectIds = projectStorage.getProjects()
        for (projectId in storageProjectIds - projectIds) {
            log.info("Removing project $projectId")
            val deploymentStorage = getDeploymentStorage(projectId, configuration.redis)
            val cohortStorage = getCohortStorage(
                projectId,
                configuration.redis,
                configuration.cohortSyncIntervalMillis.toDuration(DurationUnit.MILLISECONDS)
            )
            // Remove all deployments for project
            val deployments = deploymentStorage.getDeployments()
            for ((deploymentKey, _) in deployments) {
                log.info("Removing deployment and flag configs for deployment $deploymentKey for project $projectId")
                deploymentStorage.removeDeployment(deploymentKey)
                deploymentStorage.removeAllFlags(deploymentKey)
            }
            // Remove all cohorts for project
            val cohortDescriptions = cohortStorage.getCohortDescriptions().values
            for (cohortDescription in cohortDescriptions) {
                cohortStorage.removeCohort(cohortDescription)
            }
            projectStorage.removeProject(projectId)
        }

        /*
         * Start all project proxies.
         */
        projectProxies.map { scope.launch { it.value.start() } }.joinAll()

        /*
         * Periodically update the local cache of deployments to project values.
         */
        scope.launch {
            while (true) {
                delay(configuration.deploymentSyncIntervalMillis)
                for ((project, projectProxy) in projectProxies) {
                    val deployments = projectProxy.getDeployments().associateWith { project }
                    mutex.withLock { deploymentKeysToProject.putAll(deployments) }
                }
            }
        }
        log.info("Evaluation proxy started.")
    }

    suspend fun shutdown() = coroutineScope {
        log.info("Shutting down evaluation proxy.")
        projectProxies.map { launch { it.value.shutdown() } }.joinAll()
        supervisor.cancelAndJoin()
        log.info("Evaluation proxy shut down.")
    }

    // Apis

    suspend fun getFlagConfigs(deploymentKey: String?): List<EvaluationFlag> {
        val projectProxy = getProjectProxy(deploymentKey)
        return projectProxy.getFlagConfigs(deploymentKey)
    }

    suspend fun getCohortDescription(deploymentKey: String?, cohortId: String?): CohortDescription {
        val projectProxy = getProjectProxy(deploymentKey)
        return projectProxy.getCohortDescription(cohortId)
    }

    suspend fun getCohortMembers(deploymentKey: String?, cohortId: String?): Set<String> {
        val projectProxy = getProjectProxy(deploymentKey)
        return projectProxy.getCohortMembers(cohortId)
    }

    suspend fun getCohortMembershipsForUser(deploymentKey: String?, userId: String?): Set<String> {
        val projectProxy = getProjectProxy(deploymentKey)
        return projectProxy.getCohortMembershipsForUser(deploymentKey, userId)
    }

    suspend fun getCohortMembershipsForGroup(deploymentKey: String?, groupType: String?, groupName: String?): Set<String> {
        val projectProxy = getProjectProxy(deploymentKey)
        return projectProxy.getCohortMembershipsForGroup(deploymentKey, groupType, groupName)
    }

    suspend fun evaluate(
        deploymentKey: String?,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null
    ): Map<String, EvaluationVariant> {
        val projectProxy = getProjectProxy(deploymentKey)
        return Metrics.with({ Evaluation }, { e -> EvaluationFailure(e) }) {
            projectProxy.evaluate(deploymentKey, user, flagKeys)
        }
    }

    suspend fun evaluateV1(
        deploymentKey: String?,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null
    ): Map<String, EvaluationVariant> {
        val projectProxy = getProjectProxy(deploymentKey)
        return Metrics.with({ Evaluation }, { e -> EvaluationFailure(e) }) {
            projectProxy.evaluateV1(deploymentKey, user, flagKeys)
        }
    }

    // Private

    private suspend fun getProjectProxy(deploymentKey: String?): ProjectProxy {
        val cachedProject = mutex.withLock {
            deploymentKeysToProject[deploymentKey]
        } ?: throw HttpErrorResponseException(401, "Invalid deployment key.")
        return projectProxies[cachedProject] ?: throw HttpErrorResponseException(404, "Project not found.")
    }
}

// Serialized Proxy Calls

suspend fun EvaluationProxy.getSerializedCohortDescription(deploymentKey: String?, cohortId: String?): String =
    json.encodeToString(getCohortDescription(deploymentKey, cohortId))

suspend fun EvaluationProxy.getSerializedCohortMembers(deploymentKey: String?, cohortId: String?): String =
    json.encodeToString(getCohortMembers(deploymentKey, cohortId))

suspend fun EvaluationProxy.getSerializedFlagConfigs(deploymentKey: String?): String =
    json.encodeToString(getFlagConfigs(deploymentKey))

suspend fun EvaluationProxy.getSerializedCohortMembershipsForUser(deploymentKey: String?, userId: String?): String =
    json.encodeToString(getCohortMembershipsForUser(deploymentKey, userId))

suspend fun EvaluationProxy.getSerializedCohortMembershipsForGroup(deploymentKey: String?, groupType: String?, groupName: String?): String =
    json.encodeToString(getCohortMembershipsForGroup(deploymentKey, groupType, groupName))

suspend fun EvaluationProxy.serializedEvaluate(
    deploymentKey: String?,
    user: Map<String, Any?>?,
    flagKeys: Set<String>? = null
): String = json.encodeToString(evaluate(deploymentKey, user, flagKeys))

suspend fun EvaluationProxy.serializedEvaluateV1(
    deploymentKey: String?,
    user: Map<String, Any?>?,
    flagKeys: Set<String>? = null
): String = json.encodeToString(evaluateV1(deploymentKey, user, flagKeys))
