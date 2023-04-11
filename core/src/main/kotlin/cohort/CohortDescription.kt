package com.amplitude.cohort

import kotlinx.serialization.Serializable

@Serializable
data class CohortDescription(
    val id: String,
    val lastComputed: Long,
    val size: Int
)
