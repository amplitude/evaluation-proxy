package com.amplitude.cohort

import com.amplitude.RedisConfiguration
import com.amplitude.util.json
import com.amplitude.util.logger
import com.amplitude.util.redis.Redis
import com.amplitude.util.redis.RedisKey
import com.amplitude.util.redis.createRedisConnections
import com.squareup.moshi.JsonWriter
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import okio.buffer
import okio.sink
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.GZIPOutputStream
import kotlin.time.Duration

// Constants
private const val REDIS_SCAN_CHUNK_SIZE: Int = 1000

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
     * Get a pre-gzipped JSON blob for the given cohortId at its latest lastModified.
     */
    suspend fun getCohortBlob(cohortId: String): ByteArray?

    /**
     * Attempt to acquire a distributed lock for cohort loading.
     * Returns true if lock was acquired, false if another instance is already loading.
     */
    suspend fun tryLockCohortLoading(
        cohortId: String,
        lockTimeoutSeconds: Int,
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
            redisConfiguration.pipelineBatchSize,
            CohortBlobCache(),
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

    override suspend fun getCohortBlob(cohortId: String): ByteArray? {
        val cohort = getCohort(cohortId) ?: return null
        val baos = ByteArrayOutputStream()
        GZIPOutputStream(baos, false).use { gz ->
            val sink = gz.sink().buffer()
            val jw = JsonWriter.of(sink)

            jw.beginObject()
            jw.name("cohortId").value(cohort.id)
            jw.name("groupType").value(cohort.groupType)
            jw.name("lastModified").value(cohort.lastModified)
            jw.name("size").value(cohort.size.toLong())

            jw.name("memberIds").beginArray()
            for (id in cohort.members) jw.value(id)
            jw.endArray()

            jw.endObject()
            jw.flush()
            sink.flush()
        }
        return baos.toByteArray()
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
    private val pipelineBatchSize: Int,
    private val cohortBlobCache: CohortBlobCache,
) : CohortStorage {
    companion object {
        val log by logger()
    }

    // Track inflight blob loads to avoid duplicate reads
    private val inflightBlobLoads = ConcurrentHashMap<String, CompletableDeferred<ByteArray?>>()

    /**
     * Stream a Redis Set via SSCAN and pipeline membership updates in sub-batches.
     * The provided [pipeline] function is invoked once per SSCAN chunk with the
     * mapped updates and the configured [pipelineBatchSize].
     */
    private suspend fun processMembershipUpdates(
        sourceKey: RedisKey,
        groupType: String,
        cohortId: String,
        pipeline: suspend (updates: List<Pair<RedisKey, Set<String>>>, batchSize: Int) -> Unit,
    ) {
        val cohortIdSet = setOf(cohortId)
        redis.sscanChunked(sourceKey, chunkSize = REDIS_SCAN_CHUNK_SIZE) { userChunk ->
            if (userChunk.isEmpty()) return@sscanChunked
            val updates =
                userChunk.map { userId ->
                    RedisKey.UserCohortMemberships(
                        prefix,
                        projectId,
                        groupType,
                        userId,
                    ) to cohortIdSet
                }
            if (updates.isNotEmpty()) {
                pipeline(updates, pipelineBatchSize)
            }
        }
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
        cohortBlobCache.remove(description.id)
        redis.hdel(RedisKey.CohortDescriptions(prefix, projectId), description.id)
        val cohortMembersKey =
            RedisKey.CohortMembers(
                prefix,
                projectId,
                description.id,
                description.groupType,
                description.lastModified,
            )
        processMembershipUpdates(cohortMembersKey, description.groupType, description.id) { updates, batchSize ->
            redis.sremPipeline(updates, batchSize)
        }
        redis.del(cohortMembersKey)
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
                // If nothing changed (same or older lastModified), skip creating/updating the temp set
                val prev = existingDescription
                if (prev != null && description.lastModified <= prev.lastModified) {
                    return
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

                            // Process added users in streamed chunks
                            if (addedCount > 0) {
                                processMembershipUpdates(addedKey, description.groupType, description.id) { updates, batchSize ->
                                    redis.saddPipeline(updates, batchSize)
                                }
                            }

                            // Process removed users in streamed chunks
                            if (removedCount > 0) {
                                processMembershipUpdates(removedKey, description.groupType, description.id) { updates, batchSize ->
                                    redis.sremPipeline(updates, batchSize)
                                }
                            }
                        } finally {
                            // Clean up temporary keys
                            redis.del(addedKey)
                            redis.del(removedKey)
                            redis.expire(existingCohortKey, ttl)
                        }
                    } else {
                        // No previous cohort: all members are additions
                        processMembershipUpdates(newCohortKey, description.groupType, description.id) { updates, batchSize ->
                            redis.saddPipeline(updates, batchSize)
                        }
                    }
                }

                // Build and store a pre-gzipped JSON blob for this cohort version in Redis for fast fanout.
                val cohortId = description.id
                val cohortLastModified = description.lastModified
                val blobKey = RedisKey.CohortBlob(prefix, projectId, cohortId, cohortLastModified)
                val gzBytes = buildCohortBlobGzip(description, finalSize)
                val b64 = Base64.getEncoder().encodeToString(gzBytes)
                redis.set(blobKey, b64)

                // Remove the old blob from the cache - it will be fetched again in the next /cohort/{cohortId} request
                cohortBlobCache.remove(cohortId)

                // Publish the cohort description only after successful blob store
                val updatedDescription = description.copy(size = finalSize)
                val jsonEncodedDescription = json.encodeToString(updatedDescription)
                redis.hset(
                    RedisKey.CohortDescriptions(prefix, projectId),
                    mapOf(description.id to jsonEncodedDescription),
                )
            }
        }
    }

    /**
     * Build a gzipped JSON blob for this cohort version.
     */
    private suspend fun buildCohortBlobGzip(
        description: CohortDescription,
        finalSize: Int,
    ): ByteArray {
        val baos = ByteArrayOutputStream()
        GZIPOutputStream(baos, false).use { gz ->
            val sink = gz.sink().buffer()
            val jw = JsonWriter.of(sink)

            jw.beginObject()
            jw.name("cohortId").value(description.id)
            jw.name("groupType").value(description.groupType)
            jw.name("lastModified").value(description.lastModified)
            jw.name("size").value(finalSize.toLong())

            jw.name("memberIds").beginArray()
            val newKey =
                RedisKey.CohortMembers(
                    prefix,
                    projectId,
                    description.id,
                    description.groupType,
                    description.lastModified,
                )
            readOnlyRedis.sscanChunked(newKey, REDIS_SCAN_CHUNK_SIZE) { chunk ->
                for (id in chunk) jw.value(id)
            }
            jw.endArray()

            jw.endObject()
            jw.flush()
            sink.flush()
        }
        return baos.toByteArray()
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

    override suspend fun getCohortBlob(cohortId: String): ByteArray? {
        val description = getCohortDescription(cohortId) ?: return null
        val cohortKey = description.id
        cohortBlobCache.get(cohortKey)?.let {
            return it
        }
        // Attempt to read from Redis blob key only (read-through) with single-flight
        val newDeferred = CompletableDeferred<ByteArray?>()
        val existing = inflightBlobLoads.putIfAbsent(cohortKey, newDeferred)
        if (existing != null) {
            return existing.await()
        } else {
            try {
                val blobKey = RedisKey.CohortBlob(prefix, projectId, description.id, description.lastModified)
                val b64 = readOnlyRedis.get(blobKey)
                val gz = b64?.let { runCatching { Base64.getDecoder().decode(it) }.getOrNull() }
                if (gz != null) {
                    cohortBlobCache.put(cohortKey, gz)
                }
                newDeferred.complete(gz)
                return gz
            } catch (t: Throwable) {
                newDeferred.completeExceptionally(t)
                throw t
            } finally {
                inflightBlobLoads.remove(cohortKey, newDeferred)
            }
        }
    }

    private suspend fun getCohortMembers(
        cohortId: String,
        cohortGroupType: String,
        cohortLastModified: Long,
    ): Set<String>? {
        return readOnlyRedis.sscan(
            RedisKey.CohortMembers(prefix, projectId, cohortId, cohortGroupType, cohortLastModified),
            scanLimit,
        )
    }
}
