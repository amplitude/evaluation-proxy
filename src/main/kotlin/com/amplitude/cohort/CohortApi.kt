package com.amplitude.cohort

import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.retry
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.request.get
import io.ktor.client.request.headers
import io.ktor.client.request.parameter
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import java.util.Base64

@Serializable
private data class SerialCohortDescription(
    @SerialName("lastComputed") val lastComputed: Long,
    @SerialName("published") val published: Boolean,
    @SerialName("archived") val archived: Boolean,
    @SerialName("appId") val appId: Int,
    @SerialName("lastMod") val lastMod: Long,
    @SerialName("type") val type: String,
    @SerialName("id") val id: String,
    @SerialName("size") val size: Int,
)

@Serializable
private data class GetCohortDescriptionsResponse(
    @SerialName("cohorts") val cohorts: List<SerialCohortDescription>,
)

@Serializable
private data class GetCohortMembersResponse(
    @SerialName("cohort") val cohort: SerialCohortDescription,
    @SerialName("user_ids") val userIds: List<String?>,
)

interface CohortApi {
    suspend fun getCohortDescriptions(): List<CohortDescription>
    suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String>
}

class CohortApiV3(apiKey: String, secretKey: String) : CohortApi {

    companion object {
        val log by logger()
    }

    private val basicAuth = Base64.getEncoder().encodeToString("$apiKey:$secretKey".toByteArray(Charsets.UTF_8))
    private val client = HttpClient(OkHttp)

    override suspend fun getCohortDescriptions(): List<CohortDescription> {
        log.debug("getCohortDescriptions: start")
        val response = retry(onFailure = { e -> log.info("Get cohort descriptions failed: $e") }) {
            client.get("https://cohort.lab.amplitude.com/api/3/cohorts") {
                headers { set("Authorization", "Basic $basicAuth") }
            }
        }
        val body = json.decodeFromString<GetCohortDescriptionsResponse>(response.body())
        return body.cohorts.map { CohortDescription(id = it.id, lastComputed = it.lastComputed, size = it.size) }
            .also { log.debug("getCohortDescriptions: end - result=$it") }
    }

    // TODO configure the timeout for long running requests
    override suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String> {
        log.debug("getCohortMembers: start - cohortDescription=$cohortDescription")
        val response = retry(onFailure = { e -> log.info("Get cohort members failed: $e") }) {
            client.get("https://cohort.lab.amplitude.com/api/3/cohorts/${cohortDescription.id}") {
                headers { set("Authorization", "Basic $basicAuth") }
                parameter("lastComputed", cohortDescription.lastComputed)
                parameter("refreshCohort", false)
                parameter("amp_ids", false)
            }
        }
        val body = json.decodeFromString<GetCohortMembersResponse>(response.body())
        return body.userIds.filterNotNull().toSet()
            .also { log.debug("getCohortMembers: end - resultSize=${it.size}") }
    }
}
