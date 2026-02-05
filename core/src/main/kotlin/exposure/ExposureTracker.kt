package com.amplitude.exposure

import com.amplitude.Amplitude
import com.amplitude.Event
import com.amplitude.ExposureConfiguration
import com.amplitude.ExposureEvent
import com.amplitude.ExposureEventFilter
import com.amplitude.ExposureEventSend
import com.amplitude.ExposureEventSendFailure
import com.amplitude.Metrics
import com.amplitude.util.deviceId
import com.amplitude.util.groups
import com.amplitude.util.logger
import com.amplitude.util.userId
import org.json.JSONObject

private object FlagType {
    const val RELEASE = "release"
    const val EXPERIMENT = "experiment"
    const val MUTUAL_EXCLUSION_GROUP = "mutual-exclusion-group"
    const val HOLDOUT_GROUP = "holdout-group"
    const val RELEASE_GROUP = "release-group"
}

internal interface ExposureTracker {
    suspend fun track(exposure: Exposure)
}

internal class AmplitudeExposureTracker(
    private val amplitude: Amplitude,
    private val exposureFilter: ExposureFilter,
) : ExposureTracker {
    companion object {
        val log by logger()
    }

    constructor(
        apiKey: String,
        serverUrl: String,
        config: ExposureConfiguration,
    ) : this(
        amplitude =
            Amplitude.getInstance("exposure-$apiKey").apply {
                setServerUrl(serverUrl)
                setEventUploadThreshold(config.eventUploadThreshold)
                setEventUploadPeriodMillis(config.eventUploadPeriodMillis)
                useBatchMode(config.useBatchMode)
                init(apiKey)
            },
        exposureFilter = InMemoryExposureFilter(config.filterCapacity),
    )

    override suspend fun track(exposure: Exposure) {
        try {
            Metrics.track(ExposureEvent)
            if (exposureFilter.shouldTrack(exposure)) {
                Metrics.with({ ExposureEventSend }, { e -> ExposureEventSendFailure(e) }) {
                    exposure.toAmplitudeEvents().forEach { event ->
                        amplitude.logEvent(event)
                    }
                }
            } else {
                Metrics.track(ExposureEventFilter)
            }
        } catch (e: Exception) {
            log.error("Failed to track exposure event", e)
        }
    }
}

internal fun Exposure.toAmplitudeEvents(): List<Event> {
    val events = mutableListOf<Event>()
    val canonicalizedExposure = this.canonicalize()

    for ((flagKey, variant) in this.results) {
        // Skip if variant is not deployed
        val isDeployed = variant.metadata?.get("deployed") as? Boolean ?: false
        if (!isDeployed) {
            continue
        }

        // Skip default variant exposures
        val isDefault = variant.metadata?.get("default") as? Boolean ?: false
        if (isDefault) {
            continue
        }

        val event =
            Event(
                "[Experiment] Exposure",
                this.context.userId(),
                this.context.deviceId(),
            )

        val groups = this.context.groups()
        if (!groups.isNullOrEmpty()) {
            event.groups = JSONObject(groups)
        }

        event.eventProperties =
            JSONObject().apply {
                put("[Experiment] Flag Key", flagKey)
                if (variant.key != null) {
                    put("[Experiment] Variant", variant.key)
                }
                if (variant.metadata != null) {
                    put("metadata", JSONObject(variant.metadata))
                }
            }

        event.userProperties =
            JSONObject().apply {
                val set = JSONObject()
                val unset = JSONObject()
                val flagType = variant.metadata?.get("flagType") as? String
                if (flagType != FlagType.MUTUAL_EXCLUSION_GROUP) {
                    if (variant.key != null) {
                        set.put("[Experiment] $flagKey", variant.key)
                    }
                }
                put("\$set", set)
                put("\$unset", unset)
            }

        // Insert ID includes flagKey to make it unique per flag
        val hash = ("$flagKey $canonicalizedExposure").hashCode()
        val day = this.timestamp / DAY_MILLIS
        event.insertId = "${this.context.userId()} ${this.context.deviceId()} $hash $day"

        events.add(event)
    }

    return events
}
