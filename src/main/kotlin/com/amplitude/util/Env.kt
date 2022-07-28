package com.amplitude.util

internal fun stringEnv(variable: String): String? {
    return System.getenv(variable)
}

internal fun longEnv(variable: String): Long? {
    val stringEnv = stringEnv(variable)
    return try {
        stringEnv?.toLong()
    } catch (_: NumberFormatException) { null }
}

internal fun intEnv(variable: String): Int? {
    val stringEnv = stringEnv(variable)
    return try {
        stringEnv?.toInt()
    } catch (_: NumberFormatException) { null }
}

internal fun booleanEnv(variable: String): Boolean {
    val stringEnv = stringEnv(variable)
    return try {
        stringEnv?.toBoolean() ?: false
    } catch (_: NumberFormatException) { false }
}
