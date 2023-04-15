package com.amplitude.cohort

import com.amplitude.RedisConfiguration
import com.amplitude.util.RedisConnection
import com.amplitude.util.RedisKey
import com.amplitude.util.json
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlin.time.Duration

interface CohortStorage {
    suspend fun getCohortDescription(cohortId: String): CohortDescription?
    suspend fun getCohortDescriptions(): Map<String, CohortDescription>
    suspend fun getCohortMembershipsForUser(userId: String, cohortIds: Set<String>? = null): Set<String>
    suspend fun putCohort(description: CohortDescription, members: Set<String>)
    suspend fun removeCohort(cohortDescription: CohortDescription)
}

fun getCohortStorage(projectId: String, redisConfiguration: RedisConfiguration?, ttl: Duration): CohortStorage {
    return if (redisConfiguration == null) {
        InMemoryCohortStorage()
    } else {
        RedisCohortStorage(projectId, redisConfiguration, ttl)
    }
}

class InMemoryCohortStorage : CohortStorage {

    private class Cohort(
        val description: CohortDescription,
        val members: Set<String>
    )

    private val lock = Mutex()
    private val cohorts = mutableMapOf<String, Cohort>()

    override suspend fun getCohortDescription(cohortId: String): CohortDescription? {
        return lock.withLock { cohorts[cohortId]?.description }
    }

    override suspend fun getCohortDescriptions(): Map<String, CohortDescription> {
        return lock.withLock { cohorts.mapValues { it.value.description } }
    }

    override suspend fun putCohort(description: CohortDescription, members: Set<String>) {
        return lock.withLock { cohorts[description.id] = Cohort(description, members) }
    }

    override suspend fun removeCohort(cohortDescription: CohortDescription) {
        lock.withLock { cohorts.remove(cohortDescription.id) }
    }

    override suspend fun getCohortMembershipsForUser(userId: String, cohortIds: Set<String>?): Set<String> {
        return lock.withLock {
            (cohortIds ?: cohorts.keys).mapNotNull { id ->
                when (cohorts[id]?.members?.contains(userId)) {
                    true -> id
                    else -> null
                }
            }.toSet()
        }
    }
}

class RedisCohortStorage(
    private val projectId: String,
    redisConfiguration: RedisConfiguration,
    private val ttl: Duration
) : CohortStorage {

    private val redis = RedisConnection(redisConfiguration.uri, redisConfiguration.prefix)
    private val readOnlyRedis = RedisConnection(redisConfiguration.readOnlyUri, redisConfiguration.prefix)

    override suspend fun getCohortDescription(cohortId: String): CohortDescription? {
        val jsonEncodedDescription = redis.hget(RedisKey.CohortDescriptions(projectId), cohortId) ?: return null
        return json.decodeFromString(jsonEncodedDescription)
    }

    override suspend fun getCohortDescriptions(): Map<String, CohortDescription> {
        val jsonEncodedDescriptions = redis.hgetall(RedisKey.CohortDescriptions(projectId))
        return jsonEncodedDescriptions?.mapValues { json.decodeFromString(it.value) } ?: mapOf()
    }

    override suspend fun getCohortMembershipsForUser(userId: String, cohortIds: Set<String>?): Set<String> {
        val descriptions = getCohortDescriptions()
        val memberships = mutableSetOf<String>()
        for (description in descriptions.values) {
            // High volume, use read connection
            val isMember = readOnlyRedis.sismember(RedisKey.CohortMembers(projectId, description), userId)
            if (isMember) {
                memberships += description.id
            }
        }
        return memberships
    }

    override suspend fun putCohort(description: CohortDescription, members: Set<String>) {
        val jsonEncodedDescription = json.encodeToString(description)
        val existingDescription = getCohortDescription(description.id)
        if ((existingDescription?.lastComputed ?: 0L) < description.lastComputed) {
            redis.sadd(RedisKey.CohortMembers(projectId, description), members)
            redis.hset(RedisKey.CohortDescriptions(projectId), mapOf(description.id to jsonEncodedDescription))
            if (existingDescription != null) {
                redis.expire(RedisKey.CohortMembers(projectId, existingDescription), ttl)
            }
        }
    }

    override suspend fun removeCohort(cohortDescription: CohortDescription) {
        redis.hdel(RedisKey.CohortDescriptions(projectId), cohortDescription.id)
        redis.del(RedisKey.CohortMembers(projectId, cohortDescription))
    }
}
