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

internal fun intEnv(variable: String, default: Int? = null): Int? {
    val stringEnv = stringEnv(variable) ?: return default
    return try {
        stringEnv.toInt()
    } catch (_: NumberFormatException) { default }
}

internal fun longEnv(variable: String, default: Long? = null): Long? {
    val stringEnv = stringEnv(variable) ?: return default
    return try {
        stringEnv.toLong()
    } catch (_: NumberFormatException) { default }
}
