package com.amplitude.deployment

import kotlinx.serialization.Serializable

@Serializable
internal data class Deployment(
    val id: String,
    val projectId: String,
    val label: String,
    val key: String,
)
