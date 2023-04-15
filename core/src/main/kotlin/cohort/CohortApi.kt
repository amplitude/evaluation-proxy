package com.amplitude.cohort

import com.amplitude.util.HttpErrorResponseException
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.retry
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.get
import io.ktor.client.request.headers
import io.ktor.client.request.parameter
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.HttpStatusCode
import io.ktor.utils.io.jvm.javaio.toInputStream
import kotlinx.coroutines.delay
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
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
    @SerialName("size") val size: Int
)

@Serializable
private data class GetCohortDescriptionsResponse(
    @SerialName("cohorts") val cohorts: List<SerialCohortDescription>
)

@Serializable
private data class GetCohortMembersResponse(
    @SerialName("cohort") val cohort: SerialCohortDescription,
    @SerialName("user_ids") val userIds: List<String?>
)

interface CohortApi {
    suspend fun getCohortDescriptions(cohortIds: Set<String>): List<CohortDescription>
    suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String>
}

class CohortApiV3(apiKey: String, secretKey: String) : CohortApi {

    companion object {
        val log by logger()
    }

    private val basicAuth = Base64.getEncoder().encodeToString("$apiKey:$secretKey".toByteArray(Charsets.UTF_8))
    private val client = HttpClient(OkHttp) {
        install(HttpTimeout) {
            socketTimeoutMillis = 360000
        }
    }

    override suspend fun getCohortDescriptions(cohortIds: Set<String>): List<CohortDescription> =
        client.getCohortDescriptions(basicAuth, cohortIds)

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

@Serializable
data class GetCohortAsyncResponse(
    @SerialName("cohort_id")
    val cohortId: String,
    @SerialName("request_id")
    val requestId: String,
)

class CohortApiV5(apiKey: String, secretKey: String) : CohortApi {

    companion object {
        val log by logger()
    }
    private val csvFormat = CSVFormat.RFC4180.builder().setHeader().build()
    private val basicAuth = Base64.getEncoder().encodeToString("$apiKey:$secretKey".toByteArray(Charsets.UTF_8))
    private val client = HttpClient(OkHttp) {
        install(HttpTimeout) {
            socketTimeoutMillis = 360000
        }
    }

    override suspend fun getCohortDescriptions(cohortIds: Set<String>): List<CohortDescription> =
        client.getCohortDescriptions(basicAuth, cohortIds)

    override suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String> {
        log.debug("getCohortMembers: start - cohortDescription=$cohortDescription")
        // Initiate async cohort download
        val initialResponse = client.get("https://cohort.lab.amplitude.com/api/5/cohorts/request/${cohortDescription.id}") {
            headers { set("Authorization", "Basic $basicAuth") }
            parameter("lastComputed", cohortDescription.lastComputed)
        }
        val getCohortResponse = json.decodeFromString<GetCohortAsyncResponse>(initialResponse.body())
        log.debug("getCohortMembers: requestId=${getCohortResponse.requestId}")
        // Poll until the cohort is ready for download
        while (true) {
            val statusResponse =
                client.get("https://amplitude.com/api/5/cohorts/request-status/${getCohortResponse.requestId}") {
                    headers { set("Authorization", "Basic $basicAuth") }
                }
            log.debug("getCohortMembers: status=${statusResponse.status}")
            if (statusResponse.status == HttpStatusCode.OK) {
                break
            } else if (statusResponse.status != HttpStatusCode.Accepted) {
                throw HttpErrorResponseException(statusResponse.status)
            }
            delay(1000)
        }
        // Download the cohort
        val downloadResponse =
            client.get("https://amplitude.com/api/5/cohorts/request/${getCohortResponse.requestId}/file") {
                headers { set("Authorization", "Basic $basicAuth") }
            }
        val csv = CSVParser.parse(downloadResponse.bodyAsChannel().toInputStream(), Charsets.UTF_8, csvFormat)
        return csv.map { it.get("user_id") }.filterNot { it.isNullOrEmpty() }.toSet()
            .also { log.debug("getCohortMembers: end - resultSize=${it.size}") }
    }
}

private suspend fun HttpClient.getCohortDescriptions(basicAuth: String, cohortIds: Set<String>): List<CohortDescription> {
    CohortApiV5.log.debug("getCohortDescriptions: start")
    val response = retry(onFailure = { e -> CohortApiV5.log.info("Get cohort descriptions failed: $e") }) {
        get("https://cohort.lab.amplitude.com/api/3/cohorts") {
            headers { set("Authorization", "Basic $basicAuth") }
            parameter("cohorts", cohortIds.sorted().joinToString())
        }
    }
    val body = json.decodeFromString<GetCohortDescriptionsResponse>(response.body())
    return body.cohorts.map { CohortDescription(id = it.id, lastComputed = it.lastComputed, size = it.size) }
        .also { CohortApiV5.log.debug("getCohortDescriptions: end - result=$it") }
}
