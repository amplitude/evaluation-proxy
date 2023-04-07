package com.amplitude.cohort

import com.amplitude.util.Redis
import com.amplitude.util.RedisConfiguration
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
    redisConfiguration: RedisConfiguration,
    private val ttl: Duration
) : CohortStorage {

    private val redis = Redis(redisConfiguration)

    override suspend fun getCohortDescription(cohortId: String): CohortDescription? {
        val jsonEncodedDescription = redis.hget(RedisKey.CohortDescriptions, cohortId) ?: return null
        return json.decodeFromString(jsonEncodedDescription)
    }

    override suspend fun getCohortDescriptions(): Map<String, CohortDescription> {
        val jsonEncodedDescriptions = redis.hgetall(RedisKey.CohortDescriptions)
        return jsonEncodedDescriptions?.mapValues { json.decodeFromString(it.value) } ?: mapOf()
    }

    override suspend fun getCohortMembershipsForUser(userId: String, cohortIds: Set<String>?): Set<String> {
        val descriptions = getCohortDescriptions()
        val memberships = mutableSetOf<String>()
        for (description in descriptions.values) {
            val isMember = redis.sismember(RedisKey.CohortMembers(description), userId)
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
            redis.sadd(RedisKey.CohortMembers(description), members)
            redis.hset(RedisKey.CohortDescriptions, mapOf(description.id to jsonEncodedDescription))
            if (existingDescription != null) {
                redis.expire(RedisKey.CohortMembers(existingDescription), ttl)
            }
        }
    }

    override suspend fun removeCohort(cohortDescription: CohortDescription) {
        redis.hdel(RedisKey.CohortDescriptions, cohortDescription.id)
        redis.del(RedisKey.CohortMembers(cohortDescription))
    }
}
