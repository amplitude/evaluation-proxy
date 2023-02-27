package com.amplitude.deployment

import com.amplitude.experiment.evaluation.FlagConfig
import com.amplitude.experiment.evaluation.serialization.SerialFlagConfig
import com.amplitude.util.json
import com.amplitude.util.retry
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.request.get
import io.ktor.client.request.headers
import io.ktor.client.request.parameter
import kotlinx.serialization.decodeFromString

interface DeploymentApi {
    suspend fun getFlagConfigs(deploymentKey: String): List<FlagConfig>
}

class DeploymentApiV0 : DeploymentApi {

    private val client = HttpClient(OkHttp)

    override suspend fun getFlagConfigs(deploymentKey: String): List<FlagConfig> {
        val response = retry {
            client.get("https://api.lab.amplitude.com/sdk/rules") {
                headers { set("Authorization", "Api-Key $deploymentKey") }
                parameter("eval_mode", "local")
            }
        }
        val body = json.decodeFromString<List<SerialFlagConfig>>(response.body())
        return body.map { it.convert() }
    }
}
