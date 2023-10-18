package com.amplitude.deployment

import com.amplitude.VERSION
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.util.get
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.retry
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.request.headers

interface DeploymentApi {
    suspend fun getFlagConfigs(deploymentKey: String): List<EvaluationFlag>
}

class DeploymentApiV1(
    private val serverUrl: String
) : DeploymentApi {

    companion object {
        val log by logger()
    }

    private val client = HttpClient(OkHttp)

    override suspend fun getFlagConfigs(deploymentKey: String): List<EvaluationFlag> {
        log.debug("getFlagConfigs: start - deploymentKey=$deploymentKey")
        val response = retry(onFailure = { e -> log.info("Get flag configs failed: $e") }) {
            client.get(serverUrl, "/sdk/v2/flags?v=0") {
                headers {
                    set("Authorization", "Api-Key $deploymentKey")
                    set("X-Amp-Exp-Library", "experiment-local-proxy/$VERSION")
                }
            }
        }
        return json.decodeFromString<List<EvaluationFlag>>(response.body()).also {
            log.debug("getFlagConfigs: end - deploymentKey=$deploymentKey")
        }
    }
}
