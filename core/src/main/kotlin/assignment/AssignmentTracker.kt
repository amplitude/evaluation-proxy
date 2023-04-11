package com.amplitude.assignment

import com.amplitude.Amplitude
import com.amplitude.AssignmentConfiguration
import com.amplitude.Event
import com.amplitude.experiment.evaluation.FLAG_TYPE_MUTUAL_EXCLUSION_GROUP
import org.json.JSONObject

interface AssignmentTracker {
    suspend fun track(assignment: Assignment)
}

class AmplitudeAssignmentTracker(
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
        if (assignmentFilter.shouldTrack(assignment)) {
            amplitude.logEvent(assignment.toAmplitudeEvent())
        }
    }
}

internal fun Assignment.toAmplitudeEvent(): Event {
    val event = Event(
        "[Experiment] Assignment",
        this.user.userId,
        this.user.deviceId
    )
    event.eventProperties = JSONObject().apply {
        for ((flagKey, result) in this@toAmplitudeEvent.results) {
            put("$flagKey.variant", result.variant.key)
            put("$flagKey.details", result.description)
        }
    }
    event.userProperties = JSONObject().apply {
        val set = JSONObject()
        val unset = JSONObject()
        for ((flagKey, result) in this@toAmplitudeEvent.results) {
            if (result.type == FLAG_TYPE_MUTUAL_EXCLUSION_GROUP) {
                // Dont set user properties for mutual exclusion groups.
                continue
            } else if (result.isDefaultVariant) {
                unset.put("[Experiment] $flagKey", "-")
            } else {
                set.put("[Experiment] $flagKey", result.variant.key)
            }
        }
        put("\$set", set)
        put("\$unset", unset)
    }
    event.insertId = "${this.user.userId} ${this.user.deviceId} ${this.canonicalize().hashCode()} ${this.timestamp / DAY_MILLIS}"
    return event
}
