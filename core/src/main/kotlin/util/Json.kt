package com.amplitude.util

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull

val json =
    Json {
        ignoreUnknownKeys = true
        isLenient = true
        coerceInputValues = true
        explicitNulls = false
    }

internal fun Any?.toJsonElement(): JsonElement =
    when (this) {
        null -> JsonNull
        is Map<*, *> -> toJsonObject()
        is Collection<*> -> toJsonArray()
        is Boolean -> JsonPrimitive(this)
        is Number -> JsonPrimitive(this)
        is String -> JsonPrimitive(this)
        else -> JsonPrimitive(toString())
    }

internal fun Collection<*>.toJsonArray(): JsonArray = JsonArray(map { it.toJsonElement() })

internal fun Map<*, *>.toJsonObject(): JsonObject =
    JsonObject(
        mapNotNull {
            (it.key as? String ?: return@mapNotNull null) to it.value.toJsonElement()
        }.toMap(),
    )

internal fun JsonElement.toAny(): Any? {
    return when (this) {
        is JsonPrimitive -> toAny()
        is JsonArray -> toAnyList()
        is JsonObject -> toAnyMap()
    }
}

internal fun JsonPrimitive.toAny(): Any? {
    return if (isString) {
        contentOrNull
    } else {
        booleanOrNull ?: intOrNull ?: longOrNull ?: doubleOrNull
    }
}

internal fun JsonArray.toAnyList(): List<Any?> = map { it.toAny() }

fun JsonObject.toAnyMap(): Map<String, Any?> = mapValues { it.value.toAny() }

internal object AnySerializer : KSerializer<Any?> {
    private val delegate = JsonElement.serializer()
    override val descriptor: SerialDescriptor
        get() = SerialDescriptor("Any", delegate.descriptor)

    override fun serialize(
        encoder: Encoder,
        value: Any?,
    ) {
        val jsonElement = value.toJsonElement()
        encoder.encodeSerializableValue(delegate, jsonElement)
    }

    override fun deserialize(decoder: Decoder): Any? {
        val jsonElement = decoder.decodeSerializableValue(delegate)
        return jsonElement.toAny()
    }
}
