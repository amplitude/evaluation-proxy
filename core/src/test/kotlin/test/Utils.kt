package test

import com.amplitude.cohort.Cohort
import com.amplitude.deployment.Deployment
import com.amplitude.experiment.evaluation.EvaluationCondition
import com.amplitude.experiment.evaluation.EvaluationFlag
import com.amplitude.experiment.evaluation.EvaluationOperator
import com.amplitude.experiment.evaluation.EvaluationSegment
import com.amplitude.experiment.evaluation.EvaluationVariant
import com.amplitude.project.Project
import com.amplitude.project.SerialDeployment

internal fun user(
    userId: String? = null,
    deviceId: String? = null,
    userProperties: Map<String, Any?>? = null,
    groups: Map<String, Set<String>>? = null,
    groupProperties: Map<String, Map<String, Map<String, Any?>>>? = null,
): MutableMap<String, Any?> {
    return mutableMapOf(
        "user_id" to userId,
        "device_id" to deviceId,
        "user_properties" to userProperties,
        "groups" to groups,
        "group_properties" to groupProperties,
    )
}

internal fun flag(
    flagKey: String = "flag",
    cohortIds: Set<String> = setOf("a"),
) = EvaluationFlag(
    key = flagKey,
    variants =
        mapOf(
            "off" to EvaluationVariant("off", null, null, null),
            "on" to EvaluationVariant("on", "on", null, null),
        ),
    segments =
        listOf(
            EvaluationSegment(
                conditions =
                    listOf(
                        listOf(
                            EvaluationCondition(
                                selector = listOf("context", "user", "cohort_ids"),
                                op = EvaluationOperator.SET_CONTAINS_ANY,
                                values = cohortIds,
                            ),
                        ),
                    ),
                variant = "on",
            ),
            EvaluationSegment(
                variant = "off",
            ),
        ),
)

internal fun cohort(
    id: String,
    lastModified: Long = 100,
    size: Int = 1,
    members: Set<String> = setOf("1"),
    groupType: String = "User",
) = Cohort(
    id = id,
    groupType = groupType,
    size = size,
    lastModified = lastModified,
    members = members,
)

internal fun deployment(
    key: String,
    projectId: String = "1",
) = Deployment(
    id = "1",
    projectId = projectId,
    label = "",
    key = key,
)

internal fun Deployment.toSerialDeployment(deleted: Boolean = false) =
    SerialDeployment(
        id = id,
        projectId = projectId,
        label = label,
        key = key,
        deleted = deleted,
    )

internal fun project(id: String = "1") =
    Project(
        id = id,
        apiKey = "api",
        secretKey = "secret",
        managementKey = "management",
    )
