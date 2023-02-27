package com.amplitude.util

import com.amplitude.experiment.evaluation.Allocation
import com.amplitude.experiment.evaluation.EvaluationMode
import com.amplitude.experiment.evaluation.FlagConfig
import com.amplitude.experiment.evaluation.Operator
import com.amplitude.experiment.evaluation.SegmentTargetingConfig
import com.amplitude.experiment.evaluation.UserPropertyFilter
import com.amplitude.experiment.evaluation.Variant
import com.amplitude.experiment.evaluation.serialization.SerialAllocation
import com.amplitude.experiment.evaluation.serialization.SerialEvaluationMode
import com.amplitude.experiment.evaluation.serialization.SerialFlagConfig
import com.amplitude.experiment.evaluation.serialization.SerialOperator
import com.amplitude.experiment.evaluation.serialization.SerialSegmentTargetingConfig
import com.amplitude.experiment.evaluation.serialization.SerialUserPropertyFilter
import com.amplitude.experiment.evaluation.serialization.SerialVariant
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive

val json = Json {
    ignoreUnknownKeys = true
}

internal fun Variant.toSerial(): SerialVariant {
    return SerialVariant(key = this.key)
}

internal fun SegmentTargetingConfig.toSerial(): SerialSegmentTargetingConfig {
    return SerialSegmentTargetingConfig(
        name = this.name,
        conditions = this.conditions.map { it.toSerial() },
        allocations = this.allocations.map { it.toSerial() },
        bucketingKey = this.bucketingKey
    )
}
internal fun UserPropertyFilter.toSerial(): SerialUserPropertyFilter {
    return SerialUserPropertyFilter(
        prop = this.prop,
        op = this.op.toSerial(),
        values = this.values
    )
}
internal fun Allocation.toSerial(): SerialAllocation {
    return SerialAllocation(
        percentage = this.percentage,
        weights = this.weights,
    )
}
internal fun Operator.toSerial(): SerialOperator {
    return SerialOperator.valueOf(this.toString())
}
internal fun EvaluationMode.toSerial(): SerialEvaluationMode {
    return SerialEvaluationMode.valueOf(this.toString())
}

internal fun FlagConfig.toSerial(): SerialFlagConfig {
    return SerialFlagConfig(
        flagKey = this.flagKey,
        enabled = this.enabled,
        bucketingKey = this.bucketingKey,
        bucketingSalt = this.bucketingSalt,
        defaultValue = this.defaultValue,
        variants = this.variants.map { it.toSerial() },
        variantsExclusions = this.variantsExclusions,
        variantsInclusions = this.variantsInclusions,
        allUsersTargetingConfig = this.allUsersTargetingConfig.toSerial(),
        customSegmentTargetingConfigs = this.customSegmentTargetingConfigs?.map { it.toSerial() },
        evalMode = this.evalMode.toSerial()
    )
}

internal fun Any?.toJsonElement(): JsonElement {
    return when (this) {
        is JsonElement -> this
        is String -> JsonPrimitive(this)
        is Number -> JsonPrimitive(this)
        is Boolean -> JsonPrimitive(this)
        else -> JsonNull
    }
}
