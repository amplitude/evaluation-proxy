package com.amplitude.util

internal fun stringEnv(variable: String): String? {
    return System.getenv(variable)
}

internal fun booleanEnv(variable: String, default: Boolean = false): Boolean {
    val stringEnv = stringEnv(variable) ?: return default
    return try {
        stringEnv.toBoolean()
    } catch (_: NumberFormatException) { default }
}
