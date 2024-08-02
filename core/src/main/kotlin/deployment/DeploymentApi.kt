package com.amplitude.deployment

import com.amplitude.EVALUATION_PROXY_VERSION
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.util.RetryConfig
import com.amplitude.util.get
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.retry
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.request.headers
import io.ktor.client.request.parameter

internal interface DeploymentApi {
    suspend fun getFlagConfigs(deploymentKey: String): List<EvaluationFlag>
}

internal class DeploymentApiV2(
    private val serverUrl: String,
    engine: HttpClientEngine = OkHttp.create(),
    private val retryConfig: RetryConfig = RetryConfig()
) : DeploymentApi {

    companion object {
        val log by logger()
    }

    private val client = HttpClient(engine)

    override suspend fun getFlagConfigs(deploymentKey: String): List<EvaluationFlag> {
        log.trace("getFlagConfigs: start - deploymentKey=$deploymentKey")
        val response = retry(
            config = retryConfig,
            onFailure = { e -> log.error("Get flag configs failed: $e") }
        ) {
            client.get(serverUrl, "/sdk/v2/flags") {
                parameter("v", "0")
                headers {
                    set("Authorization", "Api-Key $deploymentKey")
                    set("X-Amp-Exp-Library", "evaluation-proxy/$EVALUATION_PROXY_VERSION")
                }
            }
        }
        return json.decodeFromString<List<EvaluationFlag>>(response.body()).also {
            log.trace("getFlagConfigs: end - deploymentKey=$deploymentKey")
        }
    }
}
