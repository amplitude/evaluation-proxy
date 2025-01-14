package com.amplitude.project

import com.amplitude.RedisConfiguration
import com.amplitude.util.Redis
import com.amplitude.util.RedisConnection
import com.amplitude.util.RedisKey
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal interface ProjectStorage {
    suspend fun getProjects(): Set<String>

    suspend fun putProject(projectId: String)

    suspend fun removeProject(projectId: String)
}

internal fun getProjectStorage(redisConfiguration: RedisConfiguration?): ProjectStorage {
    val uri = redisConfiguration?.uri
    return if (uri == null) {
        InMemoryProjectStorage()
    } else {
        RedisProjectStorage(redisConfiguration.prefix, RedisConnection(uri), redisConfiguration.scanLimit)
    }
}

internal class InMemoryProjectStorage : ProjectStorage {
    private val mutex = Mutex()
    private val projectStorage = mutableSetOf<String>()

    override suspend fun getProjects(): Set<String> =
        mutex.withLock {
            projectStorage.toSet()
        }

    override suspend fun putProject(projectId: String): Unit =
        mutex.withLock {
            projectStorage.add(projectId)
        }

    override suspend fun removeProject(projectId: String): Unit =
        mutex.withLock {
            projectStorage.remove(projectId)
        }
}

internal class RedisProjectStorage(
    private val prefix: String,
    private val redis: Redis,
    private val scanLimit: Long,
) : ProjectStorage {
    override suspend fun getProjects(): Set<String> {
        return redis.sscan(RedisKey.Projects(prefix), scanLimit) ?: emptySet()
    }

    override suspend fun putProject(projectId: String) {
        redis.sadd(RedisKey.Projects(prefix), setOf(projectId))
    }

    override suspend fun removeProject(projectId: String) {
        redis.srem(RedisKey.Projects(prefix), projectId)
    }
}
