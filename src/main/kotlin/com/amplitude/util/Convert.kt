package com.amplitude.util

import com.amplitude.experiment.ExperimentUser
import com.amplitude.experiment.Variant
import com.amplitude.experiment.evaluation.serialization.SerialExperimentUser
import com.amplitude.experiment.evaluation.serialization.SerialVariant
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.floatOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull

internal fun SerialExperimentUser?.toExperimentUser(): ExperimentUser {
    return ExperimentUser.builder()
        .userId(this?.userId)
        .deviceId(this?.deviceId)
        .region(this?.region)
        .dma(this?.dma)
        .country(this?.country)
        .city(this?.city)
        .language(this?.language)
        .platform(this?.platform)
        .version(this?.version)
        .os(this?.os)
        .deviceManufacturer(this?.deviceManufacturer)
        .deviceBrand(this?.deviceBrand)
        .deviceModel(this?.deviceModel)
        .carrier(this?.carrier)
        .library(this?.library)
        .userProperties(this?.userProperties?.mapValues {
            it.value.toAny()
        })
        .build()
}

internal fun Variant.toSerialVariant(): SerialVariant {
    return SerialVariant(key = value, payload = payload.toJsonElement())
}

internal fun Any?.toJsonElement(): JsonElement {
    return when (this) {
        is String -> JsonPrimitive(this)
        is Number -> JsonPrimitive(this)
        is Boolean -> JsonPrimitive(this)
        else -> JsonNull
    }
}

internal fun JsonElement.toAny(): Any? {
    return when (this) {
        is JsonNull -> null
        is JsonObject -> this.toMap().mapValues { it.value.toAny() }
        is JsonArray -> this.toList().map { it.toAny() }
        is JsonPrimitive -> {
            if (this.isString) {
                return this.contentOrNull
            }
            this.intOrNull ?: this.longOrNull ?: this.floatOrNull ?: this.doubleOrNull ?: this.booleanOrNull
        }
    }
}
