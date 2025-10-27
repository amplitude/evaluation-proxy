package com.amplitude.util.redis

private const val STORAGE_PROTOCOL_VERSION = "v4"

internal sealed class RedisKey(val value: String) {
    data class Projects(val prefix: String) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects")

    data class Deployments(
        val prefix: String,
        val projectId: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:deployments")

    data class FlagConfigs(
        val prefix: String,
        val projectId: String,
        val deploymentKey: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:deployments:$deploymentKey:flags")

    data class CohortDescriptions(
        val prefix: String,
        val projectId: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:cohorts")

    data class CohortMembers(
        val prefix: String,
        val projectId: String,
        val cohortId: String,
        val cohortGroupType: String,
        val cohortLastModified: Long,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:{projects:$projectId:cohort:$cohortId}:$cohortGroupType:$cohortLastModified")

    /**
     * Gzipped JSON blob for a cohort (base64-encoded gzipped bytes of a JSON object).
     */
    data class CohortBlob(
        val prefix: String,
        val projectId: String,
        val cohortId: String,
        val cohortLastModified: Long,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:{projects:$projectId:cohort:$cohortId}:blob:$cohortLastModified")

    data class UserCohortMemberships(
        val prefix: String,
        val projectId: String,
        val groupType: String,
        val groupName: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:memberships:$groupType:$groupName")

    data class CohortLoadingLock(
        val prefix: String,
        val projectId: String,
        val cohortId: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:projects:$projectId:locks:cohort_loading:$cohortId")

    /**
     * Temporary key that is tied to a specific cohort. Uses the same hash tag as CohortMembers
     * so multi-key operations like SDIFFSTORE remain single-slot in Redis Cluster.
     */
    data class CohortTemporary(
        val prefix: String,
        val projectId: String,
        val cohortId: String,
        val suffix: String,
    ) : RedisKey("$prefix:$STORAGE_PROTOCOL_VERSION:{projects:$projectId:cohort:$cohortId}:temp:$suffix")
}
