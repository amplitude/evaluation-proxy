package com.amplitude.cohort

import com.amplitude.RedisConfiguration
import com.amplitude.util.redis.Redis
import com.amplitude.util.redis.RedisKey
import com.amplitude.util.redis.createRedisConnections
import com.amplitude.util.json
import com.amplitude.util.logger
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlin.time.Duration

// Constants
private const val REDIS_MEMBERSHIP_PIPELINE_CHUNK: Int = 500
private const val REDIS_DELETE_PIPELINE_CHUNK: Int = 1000

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

    suspend fun deleteCohort(description: CohortDescription)

    /**
     * Streaming writer for very large cohorts to avoid holding all members in memory.
     * Usage: create a writer via [createWriter], feed batches via [CohortIngestionWriter.addMembers],
     * and finish with [CohortIngestionWriter.commit].
     */
    fun createWriter(description: CohortDescription): CohortIngestionWriter

    /**
     * Attempt to acquire a distributed lock for cohort loading.
     * Returns true if lock was acquired, false if another instance is already loading.
     */
    suspend fun tryLockCohortLoading(
        cohortId: String,
        lockTimeoutSeconds: Int = 300,
    ): Boolean

    /**
     * Release the distributed lock for cohort loading.
     */
    suspend fun releaseCohortLoadingLock(cohortId: String)
}

// Streaming ingestion writer contract
internal interface CohortIngestionWriter {
    suspend fun addMembers(members: List<String>)

    suspend fun complete(finalSize: Int)
}

internal fun getCohortStorage(
    projectId: String,
    redisConfiguration: RedisConfiguration?,
    ttl: Duration,
): CohortStorage {
    val connections = createRedisConnections(redisConfiguration)
    return if (connections != null) {
        RedisCohortStorage(
            projectId,
            ttl,
            redisConfiguration!!.prefix,
            connections.primary,
            connections.readOnly,
            redisConfiguration.scanLimit,
        )
    } else {
        InMemoryCohortStorage()
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

    override suspend fun deleteCohort(description: CohortDescription) {
        lock.withLock { cohorts.remove(description.id) }
    }

    override fun createWriter(description: CohortDescription): CohortIngestionWriter {
        return object : CohortIngestionWriter {
            private val buffer = mutableSetOf<String>()

            override suspend fun addMembers(members: List<String>) {
                lock.withLock { buffer.addAll(members) }
            }

            override suspend fun complete(finalSize: Int) {
                val members = lock.withLock { buffer.toSet() }
                val sizeToStore = if (description.size > 0) description.size else finalSize
                val cohort = description.copy(size = sizeToStore).toCohort(members)
                lock.withLock { cohorts[cohort.id] = cohort }
            }
        }
    }

    override suspend fun tryLockCohortLoading(
        cohortId: String,
        lockTimeoutSeconds: Int,
    ): Boolean {
        // In-memory storage is single instance, so always allow locking
        return true
    }

    override suspend fun releaseCohortLoadingLock(cohortId: String) {
        // No-op for in-memory storage
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
        return readOnlyRedis.smembers(RedisKey.UserCohortMemberships(prefix, projectId, groupType, groupName))
    }

    override suspend fun deleteCohort(description: CohortDescription) {
        redis.hdel(RedisKey.CohortDescriptions(prefix, projectId), description.id)
        val members = getCohortMembers(description.id, description.groupType, description.lastModified) ?: emptySet()
        redis.sremPipeline(
            members.map {
                RedisKey.UserCohortMemberships(prefix, projectId, description.groupType, it) to setOf(description.id)
            },
            REDIS_DELETE_PIPELINE_CHUNK,
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

    override fun createWriter(description: CohortDescription): CohortIngestionWriter {
        return object : CohortIngestionWriter {
            private val newCohortKey =
                RedisKey.CohortMembers(
                    prefix,
                    projectId,
                    description.id,
                    description.groupType,
                    description.lastModified,
                )
            private var existingDescription: CohortDescription? = null

            override suspend fun addMembers(members: List<String>) {
                if (existingDescription == null) {
                    existingDescription = getCohortDescription(description.id)
                }
                if (members.isNotEmpty()) {
                    redis.sadd(newCohortKey, members.toSet())
                }
            }

            override suspend fun complete(finalSize: Int) {
                val prev = existingDescription
                log.debug("cohort={} complete: finalSize={}", description.id, finalSize)
                
                // Only process members if the cohort has any (avoid empty cohort operations)
                if (finalSize > 0) {
                    if (prev != null) {
                        val existingCohortKey =
                            RedisKey.CohortMembers(
                                prefix,
                                projectId,
                                prev.id,
                                prev.groupType,
                                prev.lastModified,
                            )

                        // Create temporary keys for differences - server-side operations
                        val addedKey =
                            RedisKey.CohortTemporary(
                                prefix,
                                projectId,
                                description.id,
                                "added_${System.currentTimeMillis()}",
                            )
                        val removedKey =
                            RedisKey.CohortTemporary(
                                prefix,
                                projectId,
                                description.id,
                                "removed_${System.currentTimeMillis()}",
                            )

                        try {
                            // Server-side set operations - no memory transfer to client!
                            val addedCount = redis.sdiffstore(addedKey, newCohortKey, existingCohortKey)
                            val removedCount = redis.sdiffstore(removedKey, existingCohortKey, newCohortKey)
                            log.info(
                                "cohort={} diff: addedCount={}, removedCount={}",
                                description.id,
                                addedCount,
                                removedCount,
                            )

                            // Process added users in chunks to avoid memory spikes
                            if (addedCount > 0) {
                                redis.sscanChunked(addedKey, chunkSize = REDIS_MEMBERSHIP_PIPELINE_CHUNK) { userChunk ->
                                    val membershipUpdates =
                                        userChunk.map { userId ->
                                            RedisKey.UserCohortMemberships(
                                                prefix,
                                                projectId,
                                                description.groupType,
                                                userId,
                                            ) to setOf(description.id)
                                        }
                                    redis.saddPipeline(membershipUpdates, REDIS_MEMBERSHIP_PIPELINE_CHUNK)
                                }
                            }

                            // Process removed users in chunks
                            if (removedCount > 0) {
                                redis.sscanChunked(removedKey, chunkSize = REDIS_MEMBERSHIP_PIPELINE_CHUNK) { userChunk ->
                                    val membershipUpdates =
                                        userChunk.map { userId ->
                                            RedisKey.UserCohortMemberships(
                                                prefix,
                                                projectId,
                                                description.groupType,
                                                userId,
                                            ) to setOf(description.id)
                                        }
                                    redis.sremPipeline(membershipUpdates, REDIS_MEMBERSHIP_PIPELINE_CHUNK)
                                }
                            }
                        } finally {
                            // Clean up temporary keys
                            redis.del(addedKey)
                            redis.del(removedKey)
                            redis.expire(existingCohortKey, ttl)
                        }
                    } else {
                        // When there is no existing cohort, all members are new additions
                        // Process all cohort members and add them to UserCohortMemberships
                        redis.sscanChunked(newCohortKey, chunkSize = REDIS_MEMBERSHIP_PIPELINE_CHUNK) { userChunk ->
                            val membershipUpdates =
                                userChunk.map { userId ->
                                    RedisKey.UserCohortMemberships(
                                        prefix,
                                        projectId,
                                        description.groupType,
                                        userId,
                                    ) to setOf(description.id)
                                }
                            redis.saddPipeline(membershipUpdates, REDIS_MEMBERSHIP_PIPELINE_CHUNK)
                        }
                    }
                }

                val jsonEncodedDescription = json.encodeToString(description.copy(size = finalSize))
                redis.hset(RedisKey.CohortDescriptions(prefix, projectId), mapOf(description.id to jsonEncodedDescription))
            }
        }
    }

    override suspend fun tryLockCohortLoading(
        cohortId: String,
        lockTimeoutSeconds: Int,
    ): Boolean {
        val lockKey = RedisKey.CohortLoadingLock(prefix, projectId, cohortId)
        log.debug("Acquiring lock for cohort $cohortId")
        return redis.acquireLock(lockKey, lockTimeoutSeconds.toLong())
    }

    override suspend fun releaseCohortLoadingLock(cohortId: String) {
        val lockKey = RedisKey.CohortLoadingLock(prefix, projectId, cohortId)
        val released = redis.releaseLock(lockKey)
        if (!released) {
            log.warn("Failed to release lock for cohort $cohortId - lock may have expired or been taken by another process")
        }
    }

    private suspend fun getCohortMembers(
        cohortId: String,
        cohortGroupType: String,
        cohortLastModified: Long,
    ): Set<String>? {
        return redis.sscan(RedisKey.CohortMembers(prefix, projectId, cohortId, cohortGroupType, cohortLastModified), scanLimit)
    }
}
