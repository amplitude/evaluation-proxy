package com.amplitude.util

import com.amplitude.experiment.evaluation.EvaluationContext

internal fun EvaluationContext.userId(): String? {
    return (this["user"] as? Map<*, *>)?.get("user_id")?.toString()
}
internal fun EvaluationContext.deviceId(): String? {
    return (this["user"] as? Map<*, *>)?.get("device_id")?.toString()
}

internal fun EvaluationContext.groups(): Map<*, *>? {
    return this["groups"] as? Map<*, *>
}

internal fun MutableMap<String, Any?>?.toEvaluationContext(): EvaluationContext {
    val context = EvaluationContext()
    if (this.isNullOrEmpty()) {
        return context
    }
    val groups = mutableMapOf<String, Map<String, Any>>()
    val userGroups = this["groups"] as? Map<*, *>
    if (!userGroups.isNullOrEmpty()) {
        for (entry in userGroups) {
            val groupType = entry.key as? String ?: continue
            val groupNames = entry.value as? Collection<*> ?: continue
            if (groupNames.isNotEmpty()) {
                val groupName = groupNames.first() as? String ?: continue
                val groupNameMap = mutableMapOf<String, Any>().apply { put("group_name", groupName) }
                val groupProperties = this.select("group_properties", groupType, groupName) as? Map<*, *>
                if (!groupProperties.isNullOrEmpty()) {
                    groupNameMap["group_properties"] = groupProperties
                }
                val groupCohortIds = this.select("group_cohort_ids", groupType, groupName) as? Map<*, *>
                if (!groupCohortIds.isNullOrEmpty()) {
                    groupNameMap["cohort_ids"] = groupCohortIds
                }
                groups[groupType] = groupNameMap
            }
        }
        context["groups"] = groups
    }
    remove("groups")
    remove("group_properties")
    context["user"] = this
    return context
}

private fun Map<*, *>.select(vararg selector: Any?): Any? {
    var map: Map<*, *> = this
    var result: Any?
    for (i in 0 until selector.size - 1) {
        val select = selector[i]
        result = map[select]
        if (result is Map<*, *>) {
            map = result
        } else {
            return null
        }
    }
    return map[selector.last()]
}
