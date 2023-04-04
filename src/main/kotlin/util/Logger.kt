package com.amplitude.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory

fun logger(name: String): Logger = LoggerFactory.getLogger(name)

fun <R : Any> R.logger(): Lazy<Logger> {
    return lazy { logger(this::class.java.name.substringBefore("\$Companion")) }
}
