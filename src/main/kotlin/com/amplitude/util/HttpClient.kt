package com.amplitude.util

import io.ktor.client.statement.HttpResponse
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.delay

class HttpErrorResponseException(
    val statusCode: HttpStatusCode,
) : Exception("HTTP error response: code=$statusCode, message=${statusCode.description}")

suspend fun retry(
    times: Int = 8,
    initialDelayMillis: Long = 100,
    maxDelay: Long = 10000,
    factor: Double = 2.0,
    block: suspend () -> HttpResponse
): HttpResponse {
    var currentDelay = initialDelayMillis
    var error: Exception? = null
    for (i in 0..times) {
        try {
            val response = block()
            if (response.status.value in 200..299) {
                return response
            } else {
                // TODO Log response with metrics
                val code = response.status.value
                if (code != 429 && code in 400..499) {
                    throw HttpErrorResponseException(response.status)
                }
            }
        } catch (e: Exception) {
            // TODO Log error with metrics
            error = e
        }
        delay(currentDelay)
        currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelay)
    }
    throw error!!
}
