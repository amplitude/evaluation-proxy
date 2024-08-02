package com.amplitude.util

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class Loader {

    private val jobsMutex = Mutex()
    private val jobs = mutableMapOf<String, Job>()

    suspend fun load(key: String, loader: suspend CoroutineScope.() -> Unit) = coroutineScope {
        jobsMutex.withLock {
            jobs.getOrPut(key) {
                launch {
                    try {
                        loader()
                    } finally {
                        jobsMutex.withLock { jobs.remove(key) }
                    }
                }
            }
        }.join()
    }
}
