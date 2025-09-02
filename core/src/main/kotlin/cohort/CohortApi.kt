package com.amplitude.cohort

import com.amplitude.EVALUATION_PROXY_VERSION
import com.amplitude.util.RetryConfig
import com.amplitude.util.get
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.retry
import com.squareup.moshi.JsonReader
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.headers
import io.ktor.client.request.parameter
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.HttpStatusCode
import io.ktor.util.toByteArray
import io.ktor.utils.io.jvm.javaio.toInputStream
import kotlinx.serialization.Serializable
import okio.buffer
import okio.source
import java.util.Base64

internal class CohortTooLargeException(cohortId: String, maxCohortSize: Int) : RuntimeException(
    "Cohort $cohortId exceeds the maximum cohort size defined in the SDK configuration $maxCohortSize",
)

internal class CohortNotModifiedException(cohortId: String) : RuntimeException(
    "Cohort $cohortId has not been modified.",
)

@Serializable
data class GetCohortResponse(
    private val cohortId: String,
    private val lastModified: Long,
    private val size: Int,
    private val groupType: String,
    private val memberIds: Set<String>? = null,
) {
    fun toCohort() =
        Cohort(
            id = cohortId,
            groupType = groupType,
            size = size,
            lastModified = lastModified,
            members = memberIds ?: emptySet(),
        )

    companion object {
        fun fromCohort(cohort: Cohort) =
            GetCohortResponse(
                cohortId = cohort.id,
                lastModified = cohort.lastModified,
                size = cohort.size,
                groupType = cohort.groupType,
                memberIds = cohort.members,
            )
    }
}

internal interface CohortApi {
    suspend fun getCohort(
        cohortId: String,
        lastModified: Long?,
        maxCohortSize: Int,
    ): Cohort

    suspend fun streamCohort(
        cohortId: String,
        lastModified: Long?,
        maxCohortSize: Int,
        storage: CohortStorage,
    )
}

internal class CohortApiV1(
    private val serverUrl: String,
    apiKey: String,
    secretKey: String,
    engine: HttpClientEngine = OkHttp.create(),
    private val retryConfig: RetryConfig = RetryConfig(),
) : CohortApi {
    companion object {
        val log by logger()
        const val batchSize: Int = 1000
    }

    private val token = Base64.getEncoder().encodeToString("$apiKey:$secretKey".toByteArray(Charsets.UTF_8))
    private val client =
        HttpClient(engine) {
            install(HttpTimeout) {
                socketTimeoutMillis = 30000
            }
        }

    override suspend fun getCohort(
        cohortId: String,
        lastModified: Long?,
        maxCohortSize: Int,
    ): Cohort {
        log.debug("getCohortMembers({}): start - maxCohortSize={}, lastModified={}", cohortId, maxCohortSize, lastModified)
        val response =
            retry(
                config = retryConfig,
                onFailure = { e -> log.error("Cohort download failed: $e") },
                acceptCodes = setOf(HttpStatusCode.NoContent, HttpStatusCode.PayloadTooLarge),
            ) {
                client.get(
                    url = serverUrl,
                    path = "sdk/v1/cohort/$cohortId",
                ) {
                    parameter("maxCohortSize", "$maxCohortSize")
                    if (lastModified != null) {
                        parameter("lastModified", "$lastModified")
                    }
                    headers {
                        set("Authorization", "Basic $token")
                        set("X-Amp-Exp-Library", "evaluation-proxy/$EVALUATION_PROXY_VERSION")
                    }
                }
            }
        log.debug("getCohortMembers({}): status={}", cohortId, response.status)
        when (response.status) {
            HttpStatusCode.NoContent -> throw CohortNotModifiedException(cohortId)
            HttpStatusCode.PayloadTooLarge -> throw CohortTooLargeException(cohortId, maxCohortSize)
            else -> return json.decodeFromString<GetCohortResponse>(response.body()).toCohort()
        }
    }

    override suspend fun streamCohort(
        cohortId: String,
        lastModified: Long?,
        maxCohortSize: Int,
        storage: CohortStorage,
    ) {
        log.debug("streamCohort({}): start - maxCohortSize={}, lastModified={}", cohortId, maxCohortSize, lastModified)
        val response =
            retry(
                config = retryConfig,
                onFailure = { e -> log.error("Cohort download failed: $e") },
                acceptCodes = setOf(HttpStatusCode.NoContent, HttpStatusCode.PayloadTooLarge),
            ) {
                client.get(
                    url = serverUrl,
                    path = "sdk/v1/cohort/$cohortId",
                ) {
                    parameter("maxCohortSize", "$maxCohortSize")
                    if (lastModified != null) {
                        parameter("lastModified", "$lastModified")
                    }
                    headers {
                        set("Authorization", "Basic $token")
                        set("X-Amp-Exp-Library", "evaluation-proxy/$EVALUATION_PROXY_VERSION")
                    }
                }
            }
        log.debug("streamCohort({}): status={}", cohortId, response.status)
        when (response.status) {
            HttpStatusCode.NoContent -> throw CohortNotModifiedException(cohortId)
            HttpStatusCode.PayloadTooLarge -> throw CohortTooLargeException(cohortId, maxCohortSize)
            else -> {
                val input = response.bodyAsChannel().toInputStream()
                val reader = JsonReader.of(input.source().buffer())

                var parsedCohortId: String? = null
                var parsedGroupType: String? = null
                var parsedLastModified: Long? = null
                var writer: CohortIngestionWriter? = null
                val batch = ArrayList<String>(batchSize)
                var memberCount = 0

                fun ensureWriter() {
                    if (writer == null) {
                        val id = checkNotNull(parsedCohortId) { "cohortId missing" }
                        val groupType = checkNotNull(parsedGroupType) { "groupType missing" }
                        val lm = checkNotNull(parsedLastModified) { "lastModified missing" }
                        writer =
                            storage.createWriter(
                                CohortDescription(
                                    id = id,
                                    groupType = groupType,
                                    size = memberCount,
                                    lastModified = lm,
                                ),
                            )
                    }
                }

                input.use {
                    reader.use {
                        reader.beginObject()
                        while (reader.hasNext()) {
                            // Parsing in streaming mode to avoid large allocations
                            when (reader.nextName()) {
                                "cohortId" -> parsedCohortId = reader.nextString()
                                "groupType" -> parsedGroupType = reader.nextString()
                                "lastModified" -> parsedLastModified = reader.nextLong()
                                "memberIds" -> {
                                    ensureWriter()
                                    reader.beginArray()
                                    while (reader.hasNext()) {
                                        batch.add(reader.nextString())
                                        memberCount += 1
                                        if (batch.size >= batchSize) {
                                            writer!!.addMembers(batch)
                                            batch.clear()
                                        }
                                    }
                                    reader.endArray()
                                }
                                else -> reader.skipValue()
                            }
                        }
                        reader.endObject()
                    }
                }
                if (batch.isNotEmpty()) {
                    ensureWriter()
                    writer!!.addMembers(batch)
                }
                ensureWriter()
                writer!!.complete(memberCount)
            }
        }
    }
}
