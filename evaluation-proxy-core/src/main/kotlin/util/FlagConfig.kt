package com.amplitude.util

import com.amplitude.experiment.evaluation.FlagConfig
import com.amplitude.experiment.evaluation.UserPropertyFilter

const val COHORT_PROP_KEY = "userdata_cohort"

fun Collection<FlagConfig>.getCohortIds(): Set<String> {
    val cohortIds = mutableSetOf<String>()
    for (flag in this) {
        cohortIds += flag.getCohortIds()
    }
    return cohortIds
}

fun FlagConfig.getCohortIds(): Set<String> {
    val cohortIds = mutableSetOf<String>()
    for (filter in this.allUsersTargetingConfig.conditions) {
        if (filter.isCohortFilter()) {
            cohortIds += filter.values
        }
    }
    val customSegments = this.customSegmentTargetingConfigs ?: listOf()
    for (segment in customSegments) {
        for (filter in segment.conditions) {
            if (filter.isCohortFilter()) {
                cohortIds += filter.values
            }
        }
    }
    return cohortIds
}

private fun UserPropertyFilter.isCohortFilter(): Boolean = this.prop == COHORT_PROP_KEY
