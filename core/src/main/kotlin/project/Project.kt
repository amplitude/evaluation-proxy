package com.amplitude.project

internal data class Project(
    val id: String,
    val apiKey: String,
    val secretKey: String,
    val managementKey: String,
) {
    // Never print credentials. This type is logged on lookup failures.
    override fun toString(): String = "Project(id=$id)"
}
