package com.amplitude.cohort

import com.amplitude.util.HttpErrorResponseException
import com.amplitude.util.get
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.retry
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
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
private data class SerialSingleCohortDescription(
    @SerialName("cohort_id") val cohortId: String,
    @SerialName("app_id") val appId: Int = 0,
    @SerialName("org_id") val orgId: Int = 0,
    @SerialName("name") val name: String? = null,
    @SerialName("size") val size: Int = Int.MAX_VALUE,
    @SerialName("description") val description: String? = null,
    @SerialName("last_computed") val lastComputed: Long = 0
)

@Serializable
data class GetCohortAsyncResponse(
    @SerialName("cohort_id") val cohortId: String,
    @SerialName("request_id") val requestId: String
)

interface CohortApi {
    suspend fun getCohortDescriptions(cohortIds: Set<String>): List<CohortDescription>
    suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String>
}

class CohortApiV5(
    private val serverUrl: String,
    apiKey: String,
    secretKey: String
) : CohortApi {

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

    override suspend fun getCohortDescriptions(cohortIds: Set<String>): List<CohortDescription> {
        log.debug("getCohortDescriptions: start")
        return cohortIds.map { cohortId ->
            val response = retry(onFailure = { e -> log.info("Get cohort descriptions failed: $e") }) {
                client.get(serverUrl, "/api/3/cohorts/info/$cohortId") {
                    headers { set("Authorization", "Basic $basicAuth") }
                }
            }
            val serialDescription = json.decodeFromString<SerialSingleCohortDescription>(response.body())
            CohortDescription(
                id = serialDescription.cohortId,
                lastComputed = serialDescription.lastComputed,
                size = serialDescription.size
            )
        }.toList().also { log.debug("getCohortDescriptions: end - result=$it") }
    }

    override suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String> {
        log.debug("getCohortMembers: start - cohortDescription=$cohortDescription")
        // Initiate async cohort download
        val initialResponse = client.get(serverUrl, "/api/5/cohorts/request/${cohortDescription.id}") {
            headers { set("Authorization", "Basic $basicAuth") }
            parameter("lastComputed", cohortDescription.lastComputed)
        }
        val getCohortResponse = json.decodeFromString<GetCohortAsyncResponse>(initialResponse.body())
        log.debug("getCohortMembers: cohortId=${cohortDescription.id}, requestId=${getCohortResponse.requestId}")
        // Poll until the cohort is ready for download
        while (true) {
            val statusResponse =
                client.get(serverUrl, "/api/5/cohorts/request-status/${getCohortResponse.requestId}") {
                    headers { set("Authorization", "Basic $basicAuth") }
                }
            log.debug("getCohortMembers: cohortId=${cohortDescription.id}, status=${statusResponse.status}")
            if (statusResponse.status == HttpStatusCode.OK) {
                break
            } else if (statusResponse.status != HttpStatusCode.Accepted) {
                throw HttpErrorResponseException(statusResponse.status)
            }
            delay(1000)
        }
        // Download the cohort
        val downloadResponse =
            client.get(serverUrl, "/api/5/cohorts/request/${getCohortResponse.requestId}/file") {
                headers { set("Authorization", "Basic $basicAuth") }
            }
        val csv = CSVParser.parse(downloadResponse.bodyAsChannel().toInputStream(), Charsets.UTF_8, csvFormat)
        return csv.map { it.get("user_id") }.filterNot { it.isNullOrEmpty() }.toSet()
            .also { log.debug("getCohortMembers: end - cohortId=${cohortDescription.id}, resultSize=${it.size}") }
    }
}
