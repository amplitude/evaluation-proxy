package com.amplitude.cohort

import com.amplitude.RedisConfiguration
import com.amplitude.util.Redis
import com.amplitude.util.RedisConnection
import com.amplitude.util.RedisKey
import com.amplitude.util.json
import com.amplitude.util.logger
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlin.time.Duration

@Serializable
internal data class CohortDescription(
    @SerialName("cohortId") val id: String,
    val groupType: String,
    val size: Int,
    val lastModified: Long,
) {
    fun toCohort(members: Set<String>): Cohort {
        return Cohort(
            id = id,
            groupType = groupType,
            size = size,
            lastModified = lastModified,
            members = members,
        )
    }
}

internal fun Cohort.toCohortDescription(): CohortDescription {
    return CohortDescription(
        id = id,
        groupType = groupType,
        size = size,
        lastModified = lastModified,
    )
}

internal interface CohortStorage {
    suspend fun getCohort(cohortId: String): Cohort?

    suspend fun getCohorts(): Map<String, Cohort>

    suspend fun getCohortDescription(cohortId: String): CohortDescription?

    suspend fun getCohortDescriptions(): Map<String, CohortDescription>

    suspend fun getCohortMemberships(
        groupType: String,
        groupName: String,
    ): Set<String>

    suspend fun putCohort(cohort: Cohort)

    suspend fun deleteCohort(description: CohortDescription)
}

internal fun getCohortStorage(
    projectId: String,
    redisConfiguration: RedisConfiguration?,
    ttl: Duration,
): CohortStorage {
    val uri = redisConfiguration?.uri
    return if (uri == null) {
        InMemoryCohortStorage()
    } else {
        val redis =
            RedisConnection(
                uri,
                redisConfiguration.connectionTimeoutMillis,
                redisConfiguration.commandTimeoutMillis,
            )
        val readOnlyRedis =
            if (redisConfiguration.readOnlyUri != null) {
                RedisConnection(
                    redisConfiguration.readOnlyUri,
                    redisConfiguration.connectionTimeoutMillis,
                    redisConfiguration.commandTimeoutMillis,
                )
            } else {
                redis
            }
        RedisCohortStorage(
            projectId,
            ttl,
            redisConfiguration.prefix,
            redis,
            readOnlyRedis,
            redisConfiguration.scanLimit,
        )
    }
}

internal class InMemoryCohortStorage : CohortStorage {
    private val lock = Mutex()
    private val cohorts = mutableMapOf<String, Cohort>()

    override suspend fun getCohort(cohortId: String): Cohort? {
        return lock.withLock { cohorts[cohortId] }
    }

    override suspend fun getCohorts(): Map<String, Cohort> {
        return lock.withLock { cohorts.toMap() }
    }

    override suspend fun getCohortDescription(cohortId: String): CohortDescription? {
        return lock.withLock { cohorts[cohortId] }?.toCohortDescription()
    }

    override suspend fun getCohortDescriptions(): Map<String, CohortDescription> {
        return lock.withLock { cohorts.toMap() }.mapValues { it.value.toCohortDescription() }
    }

    override suspend fun getCohortMemberships(
        groupType: String,
        groupName: String,
    ): Set<String> {
        val result = mutableSetOf<String>()
        lock.withLock {
            for (cohort in cohorts.values) {
                if (cohort.groupType != groupType) {
                    continue
                }
                if (cohort.members.contains(groupName)) {
                    result.add(cohort.id)
                }
            }
        }
        return result
    }

    override suspend fun putCohort(cohort: Cohort) {
        lock.withLock { cohorts[cohort.id] = cohort }
    }

    override suspend fun deleteCohort(description: CohortDescription) {
        lock.withLock { cohorts.remove(description.id) }
    }
}

internal class RedisCohortStorage(
    private val projectId: String,
    private val ttl: Duration,
    private val prefix: String,
    private val redis: Redis,
    private val readOnlyRedis: Redis,
    private val scanLimit: Long,
) : CohortStorage {
    companion object {
        val log by logger()
    }

    override suspend fun getCohort(cohortId: String): Cohort? {
        val description = getCohortDescription(cohortId) ?: return null
        val members = getCohortMembers(cohortId, description.groupType, description.lastModified)
        if (members == null) {
            log.error("Cohort description found, but members missing. $description")
            return null
        }
        return description.toCohort(members)
    }

    override suspend fun getCohorts(): Map<String, Cohort> {
        val result = mutableMapOf<String, Cohort>()
        val cohortDescriptions = getCohortDescriptions()
        for (description in cohortDescriptions.values) {
            val members = getCohortMembers(description.id, description.groupType, description.lastModified)
            if (members == null) {
                log.error("Cohort description found, but members missing. $description")
                continue
            }
            result[description.id] = description.toCohort(members)
        }
        return result
    }

    override suspend fun getCohortDescription(cohortId: String): CohortDescription? {
        val jsonEncodedDescription = redis.hget(RedisKey.CohortDescriptions(prefix, projectId), cohortId) ?: return null
        return json.decodeFromString(jsonEncodedDescription)
    }

    override suspend fun getCohortDescriptions(): Map<String, CohortDescription> {
        val jsonEncodedDescriptions = redis.hgetall(RedisKey.CohortDescriptions(prefix, projectId))
        return jsonEncodedDescriptions?.mapValues { json.decodeFromString(it.value) } ?: mapOf()
    }

    override suspend fun getCohortMemberships(
        groupType: String,
        groupName: String,
    ): Set<String> {
        return readOnlyRedis.sscan(RedisKey.UserCohortMemberships(prefix, projectId, groupType, groupName), scanLimit) ?: emptySet()
    }

    override suspend fun putCohort(cohort: Cohort) {
        val description = cohort.toCohortDescription()
        val jsonEncodedDescription = json.encodeToString(description)
        val existingDescription = getCohortDescription(description.id)
        if ((existingDescription?.lastModified ?: 0L) < description.lastModified) {
            if (cohort.members.isNotEmpty()) {
                // Set the full cohort members set
                val cohortKey =
                    RedisKey.CohortMembers(
                        prefix,
                        projectId,
                        description.id,
                        description.groupType,
                        description.lastModified,
                    )
                redis.sadd(cohortKey, cohort.members)
                val addedUsers: Set<String>
                val removedUsers: Set<String>
                if (existingDescription != null) {
                    val existingCohortKey =
                        RedisKey.CohortMembers(
                            prefix,
                            projectId,
                            existingDescription.id,
                            existingDescription.groupType,
                            existingDescription.lastModified,
                        )
                    // Determine added & removed users
                    addedUsers = redis.sdiff(cohortKey, existingCohortKey) ?: emptySet()
                    removedUsers = redis.sdiff(existingCohortKey, cohortKey) ?: emptySet()
                } else {
                    addedUsers = cohort.members
                    removedUsers = emptySet()
                }
                redis.saddPipeline(
                    addedUsers.map {
                        RedisKey.UserCohortMemberships(prefix, projectId, description.groupType, it) to setOf(description.id)
                    },
                    1000,
                )
                redis.sremPipeline(
                    removedUsers.map {
                        RedisKey.UserCohortMemberships(prefix, projectId, description.groupType, it) to setOf(description.id)
                    },
                    1000,
                )
            }
            redis.hset(RedisKey.CohortDescriptions(prefix, projectId), mapOf(description.id to jsonEncodedDescription))
            if (existingDescription != null) {
                redis.expire(
                    RedisKey.CohortMembers(
                        prefix,
                        projectId,
                        existingDescription.id,
                        existingDescription.groupType,
                        existingDescription.lastModified,
                    ),
                    ttl,
                )
            }
        }
    }

    override suspend fun deleteCohort(description: CohortDescription) {
        redis.hdel(RedisKey.CohortDescriptions(prefix, projectId), description.id)
        val members = getCohortMembers(description.id, description.groupType, description.lastModified) ?: emptySet()
        redis.sremPipeline(
            members.map {
                RedisKey.UserCohortMemberships(prefix, projectId, description.groupType, it) to setOf(description.id)
            },
            1000,
        )
        redis.del(
            RedisKey.CohortMembers(
                prefix,
                projectId,
                description.id,
                description.groupType,
                description.lastModified,
            ),
        )
    }

    private suspend fun getCohortMembers(
        cohortId: String,
        cohortGroupType: String,
        cohortLastModified: Long,
    ): Set<String>? {
        return redis.sscan(RedisKey.CohortMembers(prefix, projectId, cohortId, cohortGroupType, cohortLastModified), scanLimit)
    }
}
