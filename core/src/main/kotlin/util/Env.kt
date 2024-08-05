package com.amplitude.util

fun stringEnv(
    variable: String,
    default: String? = null,
): String? {
    return System.getenv(variable) ?: default
}

fun booleanEnv(
    variable: String,
    default: Boolean = false,
): Boolean {
    val stringEnv = stringEnv(variable) ?: return default
    return try {
        stringEnv.toBoolean()
    } catch (_: NumberFormatException) {
        default
    }
}

fun intEnv(
    variable: String,
    default: Int? = null,
): Int? {
    val stringEnv = stringEnv(variable) ?: return default
    return try {
        stringEnv.toInt()
    } catch (_: NumberFormatException) {
        default
    }
}

fun longEnv(
    variable: String,
    default: Long? = null,
): Long? {
    val stringEnv = stringEnv(variable) ?: return default
    return try {
        stringEnv.toLong()
    } catch (_: NumberFormatException) {
        default
    }
}
