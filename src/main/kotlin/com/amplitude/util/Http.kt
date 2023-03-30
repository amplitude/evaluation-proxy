package com.amplitude.util

import io.ktor.client.statement.HttpResponse
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.delay

class HttpErrorResponseException(
    val statusCode: HttpStatusCode,
) : Exception("HTTP error response: code=$statusCode, message=${statusCode.description}")

data class RetryConfig(
    val times: Int = 8,
    val initialDelayMillis: Long = 100,
    val maxDelay: Long = 10000,
    val factor: Double = 2.0,
)

suspend fun retry(
    config: RetryConfig = RetryConfig(),
    onFailure: (Exception) -> Unit = {},
    block: suspend () -> HttpResponse
): HttpResponse {
    var currentDelay = config.initialDelayMillis
    var error: Exception? = null
    for (i in 0..config.times) {
        try {
            val response = block()
            if (response.status.value in 200..299) {
                return response
            } else {
                throw HttpErrorResponseException(response.status)
            }
        } catch (e: HttpErrorResponseException) {
            val code = e.statusCode.value
            onFailure(e)
            if (code != 429 && code in 400..499) {
                throw e
            }
        } catch (e: Exception) {
            onFailure(e)
            error = e
        }
        delay(currentDelay)
        currentDelay = (currentDelay * config.factor).toLong().coerceAtMost(config.maxDelay)
    }
    throw error!!
}
