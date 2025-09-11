package com.amplitude

import com.amplitude.assignment.AmplitudeAssignmentTracker
import com.amplitude.cohort.CohortStorage
import com.amplitude.cohort.getCohortStorage
import com.amplitude.deployment.DeploymentStorage
import com.amplitude.deployment.getDeploymentStorage
import com.amplitude.project.Project
import com.amplitude.project.ProjectApi
import com.amplitude.project.ProjectApiV1
import com.amplitude.project.ProjectProxy
import com.amplitude.project.ProjectStorage
import com.amplitude.project.getProjectStorage
import com.amplitude.util.json
import com.amplitude.util.logger
import io.ktor.http.HttpStatusCode
import io.ktor.http.isSuccess
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
import org.jetbrains.annotations.VisibleForTesting
import kotlin.time.DurationUnit
import kotlin.time.toDuration

const val EVALUATION_PROXY_VERSION = "0.10.0"

class EvaluationProxyResponseException(
    val response: EvaluationProxyResponse,
) : Exception("Evaluation proxy response error: $response")

data class EvaluationProxyResponse(
    val status: HttpStatusCode,
    val body: String,
) {
    companion object {
        fun error(
            status: HttpStatusCode,
            message: String,
        ): EvaluationProxyResponse {
            return EvaluationProxyResponse(status, message)
        }

        inline fun <reified T> json(
            status: HttpStatusCode,
            response: T,
        ): EvaluationProxyResponse {
            return EvaluationProxyResponse(status, json.encodeToString<T>(response))
        }
    }
}

class EvaluationProxy internal constructor(
    private val projectConfigurations: List<ProjectConfiguration>,
    private val configuration: Configuration,
    private val projectStorage: ProjectStorage,
    metrics: MetricsHandler? = null,
) {
    constructor(
        projectConfigurations: List<ProjectConfiguration>,
        configuration: Configuration = Configuration(),
        metricsHandler: MetricsHandler? = null,
    ) : this(
        projectConfigurations,
        configuration,
        getProjectStorage(configuration.redis),
        metricsHandler,
    )

    companion object {
        val log by logger()
    }

    init {
        Metrics.handler = metrics
    }

    private val supervisor = SupervisorJob()
    private val scope = CoroutineScope(supervisor)

    @VisibleForTesting
    internal val projectProxies = mutableMapOf<Project, ProjectProxy>()
    private val apiKeysToProject = mutableMapOf<String, Project>()
    private val secretKeysToProject = mutableMapOf<String, Project>()
    private val deploymentKeysToProject = mutableMapOf<String, Project>()
    private val mutex = Mutex()

    suspend fun start() {
        log.info("Starting evaluation proxy.")
        /*
         * Fetch deployments, setup initial mappings for each project
         * configuration, and create the project proxy.
         */
        log.info("Setting up ${projectConfigurations.size} project(s)")
        for (projectConfiguration in projectConfigurations) {
            val projectApi = createProjectApi(projectConfiguration.managementKey)
            val deployments = projectApi.getDeployments()
            if (deployments.isEmpty()) {
                continue
            }
            val projectId = deployments.first().projectId
            log.info("Fetched ${deployments.size} deployments for project $projectId")
            // Add the project to local mappings.
            val project =
                Project(
                    id = projectId,
                    apiKey = projectConfiguration.apiKey,
                    secretKey = projectConfiguration.secretKey,
                    managementKey = projectConfiguration.managementKey,
                )
            apiKeysToProject[project.apiKey] = project
            secretKeysToProject[project.secretKey] = project
            for (deployment in deployments) {
                log.debug("Mapping deployment {} project {}", deployment.key, project.id)
                deploymentKeysToProject[deployment.key] = project
            }
            // Create a project proxy and add the project to storage.
            projectProxies[project] = createProjectProxy(project)
        }

        /*
         * Update project storage with configured projects, and clean up
         * projects that have been removed.
         *
         * Add all configured projects to storage.
         */
        val projectIds = projectProxies.map { it.key.id }.toSet()
        for (projectId in projectIds) {
            log.debug("Adding project $projectId")
            projectStorage.putProject(projectId)
        }
        // Remove all non-configured projects and associated data
        val storageProjectIds = projectStorage.getProjects()
        for (projectId in storageProjectIds - projectIds) {
            log.info("Removing project $projectId")
            val deploymentStorage = createDeploymentStorage(projectId)
            val cohortStorage = createCohortStorage(projectId)
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
                cohortStorage.deleteCohort(cohortDescription)
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
                    try {
                        val deployments = projectProxy.getDeployments().associateWith { project }
                        mutex.withLock { deploymentKeysToProject.putAll(deployments) }
                    } catch (t: Throwable) {
                        log.error("Periodic deployment to project cache update failed for project ${project.id}", t)
                    }
                }
            }
        }
        log.info("Evaluation proxy started.")
    }

    suspend fun shutdown() =
        coroutineScope {
            log.info("Shutting down evaluation proxy.")
            projectProxies.map { scope.launch { it.value.shutdown() } }.joinAll()
            supervisor.cancelAndJoin()
            log.info("Evaluation proxy shut down.")
        }

    // Apis

    suspend fun getFlagConfigs(deploymentKey: String?): EvaluationProxyResponse =
        Metrics.wrapRequestMetric({ EvaluationProxyGetFlagsRequest }, { EvaluationProxyGetFlagsRequestError(it) }) {
            val project =
                getProject(deploymentKey)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.Unauthorized,
                        "Invalid deployment",
                    )
            val projectProxy =
                getProjectProxy(project)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.InternalServerError,
                        "Project proxy not found for project.",
                    )
            return@wrapRequestMetric projectProxy.getFlagConfigs(deploymentKey)
        }

    suspend fun getCohort(
        apiKey: String?,
        secretKey: String?,
        cohortId: String?,
        lastModified: Long?,
        maxCohortSize: Int?,
    ): EvaluationProxyResponse =
        Metrics.wrapRequestMetric({ EvaluationProxyGetCohortRequest }, { EvaluationProxyGetCohortRequestError(it) }) {
            val project =
                getProject(apiKey, secretKey)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.Unauthorized,
                        "Invalid api or secret key",
                    )
            val projectProxy =
                getProjectProxy(project)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.InternalServerError,
                        "Project proxy not found for project.",
                    )
            return@wrapRequestMetric projectProxy.getCohort(cohortId, lastModified, maxCohortSize)
        }

    suspend fun getCohortMemberships(
        deploymentKey: String?,
        groupType: String?,
        groupName: String?,
    ): EvaluationProxyResponse =
        Metrics.wrapRequestMetric({ EvaluationProxyGetMembershipsRequest }, { EvaluationProxyGetMembershipsRequestError(it) }) {
            val project =
                getProject(deploymentKey)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.Unauthorized,
                        "Invalid deployment",
                    )
            val projectProxy =
                getProjectProxy(project)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.InternalServerError,
                        "Project proxy not found for project.",
                    )
            return@wrapRequestMetric projectProxy.getCohortMemberships(deploymentKey, groupType, groupName)
        }

    suspend fun evaluate(
        deploymentKey: String?,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null,
    ): EvaluationProxyResponse =
        Metrics.wrapRequestMetric({ EvaluationProxyEvaluationRequest }, { EvaluationProxyEvaluationRequestError(it) }) {
            val project =
                getProject(deploymentKey)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.Unauthorized,
                        "Invalid deployment",
                    )
            val projectProxy =
                getProjectProxy(project)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.InternalServerError,
                        "Project proxy not found for project.",
                    )
            return@wrapRequestMetric Metrics.with({ Evaluation }, { e -> EvaluationFailure(e) }) {
                projectProxy.evaluate(deploymentKey, user, flagKeys)
            }
        }

    suspend fun evaluateV1(
        deploymentKey: String?,
        user: Map<String, Any?>?,
        flagKeys: Set<String>? = null,
    ): EvaluationProxyResponse =
        Metrics.wrapRequestMetric({ EvaluationProxyEvaluationRequest }, { EvaluationProxyEvaluationRequestError(it) }) {
            val project =
                getProject(deploymentKey)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.Unauthorized,
                        "Invalid deployment",
                    )
            val projectProxy =
                getProjectProxy(project)
                    ?: return@wrapRequestMetric EvaluationProxyResponse.error(
                        HttpStatusCode.InternalServerError,
                        "Project proxy not found for project.",
                    )
            return@wrapRequestMetric Metrics.with({ Evaluation }, { e -> EvaluationFailure(e) }) {
                projectProxy.evaluateV1(deploymentKey, user, flagKeys)
            }
        }

    // Private

    private suspend fun getProject(deploymentKey: String?): Project? {
        val project =
            mutex.withLock {
                deploymentKeysToProject[deploymentKey]
            }
        if (project == null) {
            log.warn(
                "Unable to find project for deployment {}. Current mappings: {}",
                deploymentKey,
                deploymentKeysToProject.mapValues { it.value.id },
            )
            return null
        }
        return project
    }

    private suspend fun getProject(
        apiKey: String?,
        secretKey: String?,
    ): Project? {
        val project =
            mutex.withLock {
                apiKeysToProject[apiKey]
            }
        if (project == null) {
            log.warn("Unable to find project for api key {}. Current mappings: {}", apiKey, apiKeysToProject.mapValues { it.value.id })
            return null
        }
        if (project.secretKey != secretKey) {
            log.warn("Secret key does not match api key for project")
            return null
        }
        return project
    }

    private fun getProjectProxy(project: Project): ProjectProxy? {
        val projectProxy = projectProxies[project]
        if (projectProxy == null) {
            log.warn("Unable to find proxy for project {}", project)
        }
        return projectProxy
    }

    @VisibleForTesting
    internal fun createProjectApi(managementKey: String): ProjectApi {
        return ProjectApiV1(
            configuration.managementServerUrl,
            managementKey,
        )
    }

    @VisibleForTesting
    internal fun createProjectProxy(project: Project): ProjectProxy {
        val assignmentTracker =
            AmplitudeAssignmentTracker(
                project.apiKey,
                configuration.analyticsServerUrl,
                configuration.assignment,
            )
        val deploymentStorage = getDeploymentStorage(project.id, configuration.redis)
        val cohortStorage =
            getCohortStorage(
                project.id,
                configuration.redis,
                configuration.cohortSyncIntervalMillis.toDuration(DurationUnit.MILLISECONDS),
            )
        return ProjectProxy(
            project,
            configuration,
            assignmentTracker,
            deploymentStorage,
            cohortStorage,
        )
    }

    @VisibleForTesting
    internal fun createDeploymentStorage(projectId: String): DeploymentStorage {
        return getDeploymentStorage(projectId, configuration.redis)
    }

    @VisibleForTesting
    internal fun createCohortStorage(projectId: String): CohortStorage {
        return getCohortStorage(
            projectId,
            configuration.redis,
            configuration.cohortSyncIntervalMillis.toDuration(DurationUnit.MILLISECONDS),
        )
    }

    private suspend fun Metrics.wrapRequestMetric(
        metric: (() -> Metric)?,
        failure: ((e: Exception) -> FailureMetric)?,
        block: suspend () -> EvaluationProxyResponse,
    ): EvaluationProxyResponse {
        track(EvaluationProxyRequest)
        metric?.invoke()?.let { track(it) }
        val response = block()
        if (!response.status.isSuccess()) {
            val exception = EvaluationProxyResponseException(response)
            track(EvaluationProxyRequestError(exception))
            failure?.invoke(exception)?.let { track(it) }
        }
        return response
    }
}
