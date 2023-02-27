package com.amplitude

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.coroutines.EmptyCoroutineContext

import kotlin.test.Test

class Test {
    private val context = EmptyCoroutineContext
    private val scope = CoroutineScope(context)
    private val t = MutableStateFlow<Int>(1)
    private val deployments = MutableSharedFlow<Set<Int>>(extraBufferCapacity = 1, onBufferOverflow = BufferOverflow.DROP_OLDEST)

    @Test
    fun test(): Unit = runBlocking {
        // Emitter
        scope.launch {
            println("starting emit")
            deployments.emit(setOf(0))
            deployments.emit(setOf(1))
            deployments.emit(setOf(2))
            deployments.emit(setOf(3))
            deployments.emit(setOf(4))
            println("ended emit")
        }
        scope.launch {
            deployments.collect {
                delay(500)
                println(it)
            }
        }.join()

    }

}
