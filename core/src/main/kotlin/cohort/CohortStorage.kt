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
import kotlin.time.Duration.Companion.hours

// Constants
private const val REDIS_SCAN_CHUNK_SIZE: Int = 1000

/**
 * Maximum number of existing-set members held in proxy memory at once while diffing two cohort
 * versions. Cohorts larger than this are diffed in ceil(size / limit) hash partitions, trading one
 * extra SSCAN pass over both member sets per additional partition for bounded memory.
 */
private const val DIFF_PARTITION_MAX_MEMBERS: Int = 2_000_000

/**
 * TTL applied to a cohort version's member set (and diff temp keys) while a refresh is in
 * flight, so that a refresh which dies mid-way (Redis timeout, pod restart, etc.) cannot
 * strand the set in Redis forever. Cleared via PERSIST when the version is promoted to
 * current. Must comfortably exceed the worst-case download + diff duration for the largest
 * supported cohort.
 */
private val PENDING_VERSION_TTL = 24.hours

/**
 * How often (in SSCAN pages / pipeline flushes) the streamed diff renews the cohort-loading lock.
 * A single partition pass over a multi-million member cohort can run for minutes — longer than the
 * lock TTL — so renewing only between passes is not enough to keep a second instance out.
 */
private const val LOCK_RENEWAL_SCAN_PAGES: Int = 512

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

    /**
     * Ensure the published (current) version's member set has no expiry armed. A refresh that
     * dies between publishing the description and clearing the pending ingestion TTL leaves the
     * live set with a fuse; calling this on every not-modified sync cycle repairs that within
     * one cycle. O(1), safe to call unconditionally.
     */
    suspend fun ensureCurrentVersionPersisted(description: CohortDescription)
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
            redisConfiguration.streamedCohortDiffEnabled,
            redisConfiguration.cohortDiffPartitionMaxMembers,
            redisConfiguration.cohortDiffScanChunkSize,
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

    override suspend fun ensureCurrentVersionPersisted(description: CohortDescription) {
        // No-op for in-memory storage: nothing expires
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
    private val streamedDiffEnabled: Boolean = false,
    private val diffPartitionMaxMembers: Int = DIFF_PARTITION_MAX_MEMBERS,
    private val diffScanChunkSize: Int = REDIS_SCAN_CHUNK_SIZE,
) : CohortStorage {
    companion object {
        val log by logger()
    }

    init {
        // A non-positive partition max makes the partition-count arithmetic skip the diff loop
        // entirely (publishing a version with zero membership updates); a non-positive chunk
        // size is rejected by SSCAN. Fail at startup instead.
        require(diffPartitionMaxMembers > 0) { "diffPartitionMaxMembers must be > 0, got $diffPartitionMaxMembers" }
        require(diffScanChunkSize > 0) { "diffScanChunkSize must be > 0, got $diffScanChunkSize" }
    }

    // Track inflight blob loads to avoid duplicate reads
    private val inflightBlobLoads = ConcurrentHashMap<String, CompletableDeferred<ByteArray?>>()

    // TTLs the loading locks were acquired with, so long-running diffs can renew them
    private val loadingLockTtls = ConcurrentHashMap<String, Long>()

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

    /**
     * Compute and apply the added/removed membership updates between the existing and new cohort
     * member sets without issuing any O(cohort size) Redis command.
     *
     * Opt-in alternative to [applySdiffstoreDiff] (the default): a SDIFFSTORE over the full
     * old/new member sets executes as a single blocking command on one (single-threaded) shard —
     * multi-second for multi-million member cohorts — stalling every concurrent command on that
     * shard for its whole duration, replicas included since they re-execute the replicated
     * command. Here both sets are instead streamed via SSCAN in [diffScanChunkSize] chunks and
     * diffed client-side, so the largest single Redis command issued is one SSCAN page regardless
     * of cohort size.
     *
     * To bound proxy memory, members are hash-partitioned into ceil(size / [diffPartitionMaxMembers])
     * partitions and diffed one partition at a time: at most one partition of the existing set is
     * held in memory at once, at the cost of one extra SSCAN pass over both sets per additional
     * partition.
     *
     * Both keys are immutable at this point (the new set is fully written before [CohortIngestionWriter.complete],
     * the existing set is the previously published version), but they are scanned from the primary
     * connection to avoid misclassifying members due to replica lag on the just-written new set.
     *
     * A missed member is not a transient error here: one skipped by the existing-set scan becomes a
     * false addition, and one skipped by the new-set scan becomes a false removal of an unchanged
     * user — neither is ever repaired by a later diff. SSCAN can silently skip members when the
     * cursor is invalidated mid-scan (cluster failover or slot migration), so each scan cross-checks
     * the raw returned count against SCARD and aborts the refresh (to be retried) on a shortfall.
     * The loading lock is renewed every [LOCK_RENEWAL_SCAN_PAGES] pages so that a diff outlasting
     * the lock TTL does not let a second instance start a concurrent load; if renewal fails the
     * diff aborts rather than run unlocked.
     */
    private suspend fun applyMembershipDiff(
        existingCohortKey: RedisKey,
        newCohortKey: RedisKey,
        description: CohortDescription,
        existingSize: Int,
    ) {
        val existingCard = redis.scard(existingCohortKey)
        val newCard = redis.scard(newCohortKey)
        val cohortIdSet = setOf(description.id)
        if (existingCard == 0L) {
            applyAllAdditions(newCohortKey, description, existingSize, newCard, cohortIdSet)
            return
        }
        // Partition count is derived from the SCARD cardinalities, not the description-reported
        // sizes: a crashed ingest can leave a version key holding more members than its published
        // size, and an understated partition count would hold more than [diffPartitionMaxMembers]
        // in memory at once — the bound this partitioning exists to enforce.
        val partitions = ((maxOf(existingCard, newCard, 1L) - 1L) / diffPartitionMaxMembers).toInt() + 1
        var addedCount = 0L
        var removedCount = 0L
        var existingDistinct = 0L
        var scanPages = 0
        for (partition in 0 until partitions) {
            renewLoadingLock(description.id)
            val expectedPartitionMembers = (existingCard / partitions).toInt() + 1
            val existingPartition = HashSet<String>((expectedPartitionMembers / 0.75f).toInt() + 1)
            var existingScanned = 0L
            redis.sscanChunked(existingCohortKey, diffScanChunkSize) { chunk ->
                existingScanned += chunk.size
                if (++scanPages % LOCK_RENEWAL_SCAN_PAGES == 0) renewLoadingLock(description.id)
                for (member in chunk) {
                    if (partitions == 1 || partitionOf(member, partitions) == partition) {
                        existingPartition.add(member)
                    }
                }
            }
            checkScanComplete(description.id, existingCohortKey, existingScanned, existingCard)
            existingDistinct += existingPartition.size
            val added = mutableListOf<String>()
            var newScanned = 0L
            redis.sscanChunked(newCohortKey, diffScanChunkSize) { chunk ->
                newScanned += chunk.size
                if (++scanPages % LOCK_RENEWAL_SCAN_PAGES == 0) renewLoadingLock(description.id)
                for (member in chunk) {
                    if (partitions > 1 && partitionOf(member, partitions) != partition) {
                        continue
                    }
                    // Members present in both versions are drained from the existing partition
                    // here; whatever remains after the scan is exactly this partition's removals.
                    if (!existingPartition.remove(member)) {
                        added.add(member)
                        if (added.size >= diffScanChunkSize) {
                            addedCount +=
                                flushMembershipUpdates(added, description.groupType, cohortIdSet) { updates, batchSize ->
                                    redis.saddPipeline(updates, batchSize)
                                }
                        }
                    }
                }
            }
            checkScanComplete(description.id, newCohortKey, newScanned, newCard)
            addedCount +=
                flushMembershipUpdates(added, description.groupType, cohortIdSet) { updates, batchSize ->
                    redis.saddPipeline(updates, batchSize)
                }
            val removed = mutableListOf<String>()
            for (member in existingPartition) {
                removed.add(member)
                if (removed.size >= diffScanChunkSize) {
                    removedCount +=
                        flushMembershipUpdates(removed, description.groupType, cohortIdSet) { updates, batchSize ->
                            redis.sremPipeline(updates, batchSize)
                        }
                    if (++scanPages % LOCK_RENEWAL_SCAN_PAGES == 0) renewLoadingLock(description.id)
                }
            }
            removedCount +=
                flushMembershipUpdates(removed, description.groupType, cohortIdSet) { updates, batchSize ->
                    redis.sremPipeline(updates, batchSize)
                }
        }
        // Duplicates in an SSCAN pass can mask skipped members from the raw-count check above.
        // Each existing member hashes to exactly one partition, so the distinct members collected
        // across all passes must equal the cardinality; a shortfall means a member was skipped and
        // would have been misclassified.
        checkScanComplete(description.id, existingCohortKey, existingDistinct, existingCard)
        log.info(
            "cohort={} diff: addedCount={}, removedCount={}",
            description.id,
            addedCount,
            removedCount,
        )
    }

    /**
     * Degraded-mode update for when the previous version's members key is gone even though its
     * description is still published. SDIFFSTORE semantics for this state were "every new member
     * is an addition, nothing is removed" — match that rather than failing the refresh forever,
     * but loudly: members dropped between the two versions keep a stale membership until a later
     * update removes them.
     */
    private suspend fun applyAllAdditions(
        newCohortKey: RedisKey,
        description: CohortDescription,
        existingSize: Int,
        newCard: Long,
        cohortIdSet: Set<String>,
    ) {
        if (existingSize > 0) {
            log.error(
                "cohort={} existing members key missing at diff time (expected size {}); " +
                    "applying all new members as additions with no removals",
                description.id,
                existingSize,
            )
        }
        renewLoadingLock(description.id)
        val added = mutableListOf<String>()
        var addedCount = 0L
        var newScanned = 0L
        var scanPages = 0
        redis.sscanChunked(newCohortKey, diffScanChunkSize) { chunk ->
            newScanned += chunk.size
            if (++scanPages % LOCK_RENEWAL_SCAN_PAGES == 0) renewLoadingLock(description.id)
            for (member in chunk) {
                added.add(member)
                if (added.size >= diffScanChunkSize) {
                    addedCount +=
                        flushMembershipUpdates(added, description.groupType, cohortIdSet) { updates, batchSize ->
                            redis.saddPipeline(updates, batchSize)
                        }
                }
            }
        }
        checkScanComplete(description.id, newCohortKey, newScanned, newCard)
        addedCount +=
            flushMembershipUpdates(added, description.groupType, cohortIdSet) { updates, batchSize ->
                redis.saddPipeline(updates, batchSize)
            }
        log.info(
            "cohort={} diff: addedCount={}, removedCount={}",
            description.id,
            addedCount,
            0L,
        )
    }

    /**
     * SSCAN returns every member of an unmodified set at least once, so the raw returned count
     * (duplicates included) must be >= the set's cardinality. A shortfall means the scan was
     * silently truncated (e.g. the cursor was invalidated by a cluster failover or slot
     * migration mid-scan); diffing from a truncated scan would corrupt per-user memberships.
     */
    private fun checkScanComplete(
        cohortId: String,
        key: RedisKey,
        scanned: Long,
        cardinality: Long,
    ) {
        check(scanned >= cardinality) {
            "cohort=$cohortId SSCAN of ${key.value} returned $scanned members but SCARD reported " +
                "$cardinality; aborting diff to retry (scan was silently truncated)"
        }
    }

    private suspend fun renewLoadingLock(cohortId: String) {
        // Only renew when this instance acquired the lock (tests call complete() directly)
        val ttlSeconds = loadingLockTtls[cohortId] ?: return
        val lockKey = RedisKey.CohortLoadingLock(prefix, projectId, cohortId)
        // Fail closed: continuing without the lock lets a second instance run a concurrent
        // diff of the same cohort, interleaving membership writes. Aborting leaves the
        // description unpublished and the refresh retries next sync cycle.
        check(redis.renewLock(lockKey, ttlSeconds)) {
            "cohort=$cohortId loading-lock renewal failed (expired or taken over mid-diff); " +
                "aborting refresh to avoid interleaving with a concurrent load"
        }
    }

    private suspend fun flushMembershipUpdates(
        members: MutableList<String>,
        groupType: String,
        cohortIdSet: Set<String>,
        pipeline: suspend (updates: List<Pair<RedisKey, Set<String>>>, batchSize: Int) -> Unit,
    ): Long {
        if (members.isEmpty()) return 0L
        val updates =
            members.map { userId ->
                RedisKey.UserCohortMemberships(prefix, projectId, groupType, userId) to cohortIdSet
            }
        pipeline(updates, pipelineBatchSize)
        val flushed = members.size.toLong()
        members.clear()
        return flushed
    }

    private fun partitionOf(
        member: String,
        partitions: Int,
    ): Int {
        val hash = member.hashCode() % partitions
        return if (hash < 0) hash + partitions else hash
    }

    /**
     * Compute and apply the added/removed membership updates with two server-side SDIFFSTORE
     * commands into temporary keys. Atomic and delta-sized on the apply side, but a set
     * difference over multi-million member sets blocks its (single-threaded) shard for the
     * whole computation and can exceed the Redis command timeout; enable the streamed
     * client-side diff for cohorts at that scale.
     */
    private suspend fun applySdiffstoreDiff(
        existingCohortKey: RedisKey,
        newCohortKey: RedisKey,
        description: CohortDescription,
    ) {
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
            // Backstop TTLs are armed immediately after each SDIFFSTORE (not after both):
            // the second SDIFFSTORE is itself a multi-second blocking command on large
            // cohorts, so addedKey would otherwise sit unprotected through the likeliest
            // crash window. If this process dies before the DELs below run, the temp keys
            // self-clean instead of persisting forever.
            val addedCount = redis.sdiffstore(addedKey, newCohortKey, existingCohortKey)
            redis.expire(addedKey, PENDING_VERSION_TTL)
            val removedCount = redis.sdiffstore(removedKey, existingCohortKey, newCohortKey)
            redis.expire(removedKey, PENDING_VERSION_TTL)
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
        val blobKey = RedisKey.CohortBlob(prefix, projectId, description.id, description.lastModified)
        redis.del(blobKey)
        cohortBlobCache.remove(description.id, description.lastModified)
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
            private var pendingTtlApplied = false

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
                    if (!pendingTtlApplied) {
                        // Self-clean if this refresh dies before the version is promoted in complete()
                        redis.expire(newCohortKey, PENDING_VERSION_TTL)
                        pendingTtlApplied = true
                    }
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

                        if (streamedDiffEnabled) {
                            applyMembershipDiff(existingCohortKey, newCohortKey, description, prev.size)
                        } else {
                            applySdiffstoreDiff(existingCohortKey, newCohortKey, description)
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

                // Promote this version: publish the cohort description (only after successful
                // blob store), then clear the pending TTL so the now-current member set does
                // not expire. Publish-then-persist means a refresh that dies anywhere before
                // the publish always leaves a set that self-cleans via its pending TTL; the
                // one-command window where the published set still carries the TTL is repaired
                // by [ensureCurrentVersionPersisted] on the next sync cycle.
                val updatedDescription = description.copy(size = finalSize)
                val jsonEncodedDescription = json.encodeToString(updatedDescription)
                redis.hset(
                    RedisKey.CohortDescriptions(prefix, projectId),
                    mapOf(description.id to jsonEncodedDescription),
                )
                redis.persist(newCohortKey)

                // Retire the previous version only after successful promotion. A failed
                // refresh must leave the previous (still published) version untouched —
                // expiring it in a finally block would break reads of the live cohort.
                if (finalSize > 0 && prev != null && prev.lastModified != description.lastModified) {
                    val previousCohortKey =
                        RedisKey.CohortMembers(
                            prefix,
                            projectId,
                            prev.id,
                            prev.groupType,
                            prev.lastModified,
                        )
                    val previousBlobKey = RedisKey.CohortBlob(prefix, projectId, description.id, prev.lastModified)
                    redis.expire(previousCohortKey, ttl)
                    redis.expire(previousBlobKey, ttl)
                }
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
        val acquired = redis.acquireLock(lockKey, lockTimeoutSeconds.toLong())
        if (acquired) {
            loadingLockTtls[cohortId] = lockTimeoutSeconds.toLong()
        }
        return acquired
    }

    override suspend fun ensureCurrentVersionPersisted(description: CohortDescription) {
        redis.persist(
            RedisKey.CohortMembers(
                prefix,
                projectId,
                description.id,
                description.groupType,
                description.lastModified,
            ),
        )
    }

    override suspend fun releaseCohortLoadingLock(cohortId: String) {
        loadingLockTtls.remove(cohortId)
        val lockKey = RedisKey.CohortLoadingLock(prefix, projectId, cohortId)
        val released = redis.releaseLock(lockKey)
        if (!released) {
            log.warn("Failed to release lock for cohort $cohortId - lock may have expired or been taken by another process")
        }
    }

    override suspend fun getCohortBlob(cohortId: String): ByteArray? {
        val description = getCohortDescription(cohortId) ?: return null
        cohortBlobCache.get(description.id, description.lastModified)?.let {
            return it
        }
        // Attempt to read from Redis blob key only (read-through) with single-flight
        val inflightKey = "${description.id}:${description.lastModified}"
        val newDeferred = CompletableDeferred<ByteArray?>()
        val existing = inflightBlobLoads.putIfAbsent(inflightKey, newDeferred)
        if (existing != null) {
            return existing.await()
        } else {
            try {
                val blobKey = RedisKey.CohortBlob(prefix, projectId, description.id, description.lastModified)
                val b64 = readOnlyRedis.get(blobKey)
                val gz = b64?.let { runCatching { Base64.getDecoder().decode(it) }.getOrNull() }
                if (gz != null) {
                    cohortBlobCache.put(description.id, description.lastModified, gz)
                }
                newDeferred.complete(gz)
                return gz
            } catch (t: Throwable) {
                newDeferred.completeExceptionally(t)
                throw t
            } finally {
                inflightBlobLoads.remove(inflightKey, newDeferred)
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
