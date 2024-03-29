package com.amplitude.cohort

import com.amplitude.util.HttpErrorException
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
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.lang.IllegalArgumentException
import java.util.Base64

@Serializable
private data class SerialCohortInfoResponse(
    @SerialName("cohort_id") val cohortId: String,
    @SerialName("app_id") val appId: Int = 0,
    @SerialName("org_id") val orgId: Int = 0,
    @SerialName("name") val name: String? = null,
    @SerialName("size") val size: Int = Int.MAX_VALUE,
    @SerialName("description") val description: String? = null,
    @SerialName("last_computed") val lastComputed: Long = 0,
    @SerialName("group_type") val groupType: String = USER_GROUP_TYPE
)

@Serializable
private data class GetCohortAsyncResponse(
    @SerialName("cohort_id") val cohortId: String,
    @SerialName("request_id") val requestId: String
)

internal interface CohortApi {
    suspend fun getCohortDescription(cohortId: String): CohortDescription
    suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String>
}

internal class CohortApiV5(
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
            socketTimeoutMillis = 30000
        }
    }

    override suspend fun getCohortDescription(cohortId: String): CohortDescription {
        val response = retry(onFailure = { e -> log.info("Get cohort descriptions failed: $e") }) {
            client.get(serverUrl, "/api/3/cohorts/info/$cohortId") {
                headers { set("Authorization", "Basic $basicAuth") }
            }
        }
        val serialDescription = json.decodeFromString<SerialCohortInfoResponse>(response.body())
        return CohortDescription(
            id = serialDescription.cohortId,
            lastComputed = serialDescription.lastComputed,
            size = serialDescription.size,
            groupType = serialDescription.groupType
        )
    }

    override suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String> {
        log.debug("getCohortMembers: start - cohortDescription={}", cohortDescription)
        // Initiate async cohort download
        val initialResponse = retry(onFailure = { e -> log.error("Cohort download request failed: $e") }) {
            client.get(serverUrl, "/api/5/cohorts/request/${cohortDescription.id}") {
                headers { set("Authorization", "Basic $basicAuth") }
                parameter("lastComputed", cohortDescription.lastComputed)
            }
        }
        val getCohortResponse = json.decodeFromString<GetCohortAsyncResponse>(initialResponse.body())
        log.debug("getCohortMembers: poll for status - cohortId=${cohortDescription.id}, requestId=${getCohortResponse.requestId}")
        // Poll until the cohort is ready for download
        while (true) {
            val statusResponse = retry(onFailure = { e -> log.error("Cohort request status failed: $e") }) {
                client.get(serverUrl, "/api/5/cohorts/request-status/${getCohortResponse.requestId}") {
                    headers { set("Authorization", "Basic $basicAuth") }
                }
            }
            log.trace("getCohortMembers: cohortId={}, status={}", cohortDescription.id, statusResponse.status)
            if (statusResponse.status == HttpStatusCode.OK) {
                break
            } else if (statusResponse.status != HttpStatusCode.Accepted) {
                throw HttpErrorException(statusResponse.status, statusResponse)
            }
            delay(5000)
        }
        // Download the cohort
        log.debug("getCohortMembers: download cohort - cohortId=${cohortDescription.id}, requestId=${getCohortResponse.requestId}")
        val downloadResponse = retry(onFailure = { e -> log.error("Cohort file download failed: $e") }) {
            client.get(serverUrl, "/api/5/cohorts/request/${getCohortResponse.requestId}/file") {
                headers { set("Authorization", "Basic $basicAuth") }
            }
        }
        // Parse the csv response
        val csv = CSVParser.parse(downloadResponse.bodyAsChannel().toInputStream(), Charsets.UTF_8, csvFormat)
        return if (cohortDescription.groupType == USER_GROUP_TYPE) {
            csv.map { it.get("user_id") }.filterNot { it.isNullOrEmpty() }.toSet()
        } else {
            csv.map {
                try {
                    // CSV returned from API has all strings prefixed with a tab character
                    it.get("\tgroup_value")
                } catch (e: IllegalArgumentException) {
                    it.get("group_value")
                }
            }.filterNot {
                it.isNullOrEmpty()
            }.map {
                // CSV returned from API has all strings prefixed with a tab character
                it.removePrefix("\t")
            }.toSet()
        }.also { log.debug("getCohortMembers: end - resultSize=${it.size}") }
    }
}
