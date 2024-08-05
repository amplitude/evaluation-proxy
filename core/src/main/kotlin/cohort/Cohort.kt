package com.amplitude.cohort

internal const val USER_GROUP_TYPE = "User"

data class Cohort(
    val id: String,
    val groupType: String,
    val size: Int,
    val lastModified: Long,
    val members: Set<String>,
) {
    override fun toString(): String {
        return "Cohort(id='$id', groupType='$groupType', size=$size, lastModified=$lastModified)"
    }
}
