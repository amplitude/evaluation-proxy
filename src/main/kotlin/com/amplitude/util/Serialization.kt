package com.amplitude.util

import kotlinx.serialization.json.Json

val json = Json {
    ignoreUnknownKeys = true
}

// internal fun Variant.toSerial(): SerialVariant {
//     return SerialVariant(key = this.key)
// }
//
// internal fun SegmentTargetingConfig.toSerial(): SerialSegmentTargetingConfig {
//     return SerialSegmentTargetingConfig(
//         name = this.name,
//         conditions = this.conditions.map { it.toSerial() },
//         allocations = this.allocations.map { it.toSerial() },
//         bucketingKey = this.bucketingKey
//     )
// }
// internal fun UserPropertyFilter.toSerial(): SerialUserPropertyFilter {
//     return SerialUserPropertyFilter(
//         prop = this.prop,
//         op = this.op.toSerial(),
//         values = this.values
//     )
// }
// internal fun Allocation.toSerial(): SerialAllocation {
//     return SerialAllocation(
//         percentage = this.percentage,
//         weights = this.weights,
//     )
// }
// internal fun Operator.toSerial(): SerialOperator {
//     return SerialOperator.valueOf(this.toString())
// }
// internal fun EvaluationMode.toSerial(): SerialEvaluationMode {
//     return SerialEvaluationMode.valueOf(this.toString())
// }
//
// internal fun FlagConfig.toSerial(): SerialFlagConfig {
//     return SerialFlagConfig(
//         flagKey = this.flagKey,
//         enabled = this.enabled,
//         bucketingKey = this.bucketingKey,
//         bucketingSalt = this.bucketingSalt,
//         defaultValue = this.defaultValue,
//         variants = this.variants.map { it.toSerial() },
//         variantsExclusions = this.variantsExclusions,
//         variantsInclusions = this.variantsInclusions,
//         allUsersTargetingConfig = this.allUsersTargetingConfig.toSerial(),
//         customSegmentTargetingConfigs = this.customSegmentTargetingConfigs?.map { it.toSerial() },
//         evalMode = this.evalMode.toSerial()
//     )
// }
//
// internal fun Any?.toJsonElement(): JsonElement {
//     return when (this) {
//         is JsonElement -> this
//         is String -> JsonPrimitive(this)
//         is Number -> JsonPrimitive(this)
//         is Boolean -> JsonPrimitive(this)
//         else -> JsonNull
//     }
// }
