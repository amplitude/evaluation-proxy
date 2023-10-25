package com.amplitude.cohort

import com.amplitude.RedisConfiguration
import com.amplitude.deployment.InMemoryDeploymentStorage
import com.amplitude.deployment.RedisDeploymentStorage
import com.amplitude.util.RedisConnection
import com.amplitude.util.RedisKey
import com.amplitude.util.json
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString
import kotlin.time.Duration

internal interface CohortStorage {
    suspend fun getCohortDescription(cohortId: String): CohortDescription?
    suspend fun getCohortDescriptions(): Map<String, CohortDescription>
    suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String>?
    suspend fun getCohortMembershipsForUser(userId: String, cohortIds: Set<String>? = null): Set<String>
    suspend fun putCohort(description: CohortDescription, members: Set<String>)
    suspend fun removeCohort(cohortDescription: CohortDescription)
}

internal fun getCohortStorage(projectId: String, redisConfiguration: RedisConfiguration?, ttl: Duration): CohortStorage {
    val uri = redisConfiguration?.uri
    return if (uri == null) {
        InMemoryCohortStorage()
    } else {
        val redis = RedisConnection(uri)
        val readOnlyRedis = if (redisConfiguration.readOnlyUri != null) {
            RedisConnection(redisConfiguration.readOnlyUri)
        } else {
            redis
        }
        RedisCohortStorage(projectId, ttl, redisConfiguration.prefix, redis, readOnlyRedis)
    }
}

internal class InMemoryCohortStorage : CohortStorage {

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

    override suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String>? {
        return lock.withLock { cohorts[cohortDescription.id]?.members }
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

internal class RedisCohortStorage(
    private val projectId: String,
    private val ttl: Duration,
    private val prefix: String,
    private val redis: RedisConnection,
    private val readOnlyRedis: RedisConnection,
) : CohortStorage {

    override suspend fun getCohortDescription(cohortId: String): CohortDescription? {
        val jsonEncodedDescription = redis.hget(RedisKey.CohortDescriptions(prefix, projectId), cohortId) ?: return null
        return json.decodeFromString(jsonEncodedDescription)
    }

    override suspend fun getCohortDescriptions(): Map<String, CohortDescription> {
        val jsonEncodedDescriptions = redis.hgetall(RedisKey.CohortDescriptions(prefix, projectId))
        return jsonEncodedDescriptions?.mapValues { json.decodeFromString(it.value) } ?: mapOf()
    }

    override suspend fun getCohortMembers(cohortDescription: CohortDescription): Set<String>? {
        return redis.smembers(RedisKey.CohortMembers(prefix, projectId, cohortDescription))
    }

    override suspend fun getCohortMembershipsForUser(userId: String, cohortIds: Set<String>?): Set<String> {
        val descriptions = getCohortDescriptions()
        val memberships = mutableSetOf<String>()
        for (description in descriptions.values) {
            // High volume, use read connection
            val isMember = readOnlyRedis.sismember(RedisKey.CohortMembers(prefix, projectId, description), userId)
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
            redis.sadd(RedisKey.CohortMembers(prefix, projectId, description), members)
            redis.hset(RedisKey.CohortDescriptions(prefix, projectId), mapOf(description.id to jsonEncodedDescription))
            if (existingDescription != null) {
                redis.expire(RedisKey.CohortMembers(prefix, projectId, existingDescription), ttl)
            }
        }
    }

    override suspend fun removeCohort(cohortDescription: CohortDescription) {
        redis.hdel(RedisKey.CohortDescriptions(prefix, projectId), cohortDescription.id)
        redis.del(RedisKey.CohortMembers(prefix, projectId, cohortDescription))
    }
}
