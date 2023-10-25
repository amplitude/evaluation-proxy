package com.amplitude.util

import io.ktor.client.HttpClient
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.request
import io.ktor.client.request.url
import io.ktor.client.statement.HttpResponse
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.path
import kotlinx.coroutines.delay

internal class HttpErrorException(
    val statusCode: HttpStatusCode,
    response: HttpResponse? = null,
) : Exception("HTTP error response: code=$statusCode, message=${statusCode.description}, response=$response")

internal data class RetryConfig(
    val times: Int = 8,
    val initialDelayMillis: Long = 100,
    val maxDelay: Long = 10000,
    val factor: Double = 2.0
)

internal suspend fun retry(
    config: RetryConfig = RetryConfig(),
    onFailure: (Exception) -> Unit = {},
    block: suspend () -> HttpResponse
): HttpResponse {
    var currentDelay = config.initialDelayMillis
    var error: Exception? = null
    for (i in 0..config.times) {
        try {
            val response = block()
            if (response.status.value in 100..399) {
                return response
            } else {
                throw HttpErrorException(response.status, response)
            }
        } catch (e: HttpErrorException) {
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

internal suspend fun HttpClient.get(
    url: String,
    path: String,
    block: HttpRequestBuilder.() -> Unit
): HttpResponse {
    return request(HttpMethod.Get, url, path, block)
}

internal suspend fun HttpClient.request(
    method: HttpMethod,
    url: String,
    path: String,
    block: HttpRequestBuilder.() -> Unit
): HttpResponse {
    return request {
        this.method = method
        url {
            url(url)
            path(path)
        }
        block.invoke(this)
    }
}
