package com.amplitude.assignment

import com.amplitude.Amplitude
import com.amplitude.AssignmentConfiguration
import com.amplitude.AssignmentEvent
import com.amplitude.AssignmentEventFilter
import com.amplitude.AssignmentEventSend
import com.amplitude.AssignmentEventSendFailure
import com.amplitude.Event
import com.amplitude.Metrics
import com.amplitude.util.deviceId
import com.amplitude.util.userId
import org.json.JSONObject

private object FlagType {
    const val RELEASE = "release"
    const val EXPERIMENT = "experiment"
    const val MUTUAL_EXCLUSION_GROUP = "mutual-exclusion-group"
    const val HOLDOUT_GROUP = "holdout-group"
    const val RELEASE_GROUP = "release-group"
}

internal interface AssignmentTracker {
    suspend fun track(assignment: Assignment)
}

internal class AmplitudeAssignmentTracker(
    private val amplitude: Amplitude,
    private val assignmentFilter: AssignmentFilter
) : AssignmentTracker {

    constructor(
        apiKey: String,
        config: AssignmentConfiguration
    ) : this (
        amplitude = Amplitude.getInstance().apply {
            setEventUploadThreshold(config.eventUploadThreshold)
            setEventUploadPeriodMillis(config.eventUploadPeriodMillis)
            useBatchMode(config.useBatchMode)
            init(apiKey)
        },
        assignmentFilter = InMemoryAssignmentFilter(config.filterCapacity)
    )

    override suspend fun track(assignment: Assignment) {
        Metrics.track(AssignmentEvent)
        if (assignmentFilter.shouldTrack(assignment)) {
            Metrics.with({ AssignmentEventSend }, { e -> AssignmentEventSendFailure(e) }) {
                amplitude.logEvent(assignment.toAmplitudeEvent())
            }
        } else {
            Metrics.track(AssignmentEventFilter)
        }
    }
}

internal fun Assignment.toAmplitudeEvent(): Event {
    val event = Event(
        "[Experiment] Assignment",
        this.context.userId(),
        this.context.deviceId()
    )
    event.eventProperties = JSONObject().apply {
        for ((flagKey, variant) in this@toAmplitudeEvent.results) {
            val version = variant.metadata?.get("version")
            val segmentName = variant.metadata?.get("segmentName")
            val details = "v$version rule:$segmentName"
            put("$flagKey.variant", variant.key)
            put("$flagKey.details", details)
        }
    }
    event.userProperties = JSONObject().apply {
        val set = JSONObject()
        val unset = JSONObject()
        for ((flagKey, variant) in this@toAmplitudeEvent.results) {
            val flagType = variant.metadata?.get("flagType") as? String
            val default = variant.metadata?.get("default") as? Boolean ?: false
            if (flagType == FlagType.MUTUAL_EXCLUSION_GROUP) {
                // Dont set user properties for mutual exclusion groups.
                continue
            } else if (default) {
                unset.put("[Experiment] $flagKey", "-")
            } else {
                set.put("[Experiment] $flagKey", variant.key)
            }
        }
        put("\$set", set)
        put("\$unset", unset)
    }
    event.insertId = "${this.context.userId()} ${this.context.deviceId()} ${this.canonicalize().hashCode()} ${this.timestamp / DAY_MILLIS}"
    return event
}
