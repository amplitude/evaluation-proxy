package com.amplitude.project

import com.amplitude.RedisConfiguration
import com.amplitude.util.RedisConnection
import com.amplitude.util.RedisKey
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface ProjectStorage {
    val projects: Flow<Set<String>>
    suspend fun getProjects(): Set<String>
    suspend fun putProject(projectId: String)
    suspend fun removeProject(projectId: String)
}

fun getProjectStorage(redisConfiguration: RedisConfiguration?): ProjectStorage {
    val uri = redisConfiguration?.uri
    val prefix = redisConfiguration?.prefix
    return if (uri == null || prefix == null) {
        InMemoryProjectStorage()
    } else {
        RedisProjectStorage(uri, prefix)
    }
}

class InMemoryProjectStorage : ProjectStorage {

    override val projects = MutableSharedFlow<Set<String>>(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private val mutex = Mutex()
    private val projectStorage = mutableSetOf<String>()

    override suspend fun getProjects(): Set<String> = mutex.withLock {
        projectStorage.toSet()
    }

    override suspend fun putProject(projectId: String): Unit = mutex.withLock {
        projectStorage.add(projectId)
        projects.emit(projectStorage.toSet())
    }

    override suspend fun removeProject(projectId: String): Unit = mutex.withLock {
        projectStorage.remove(projectId)
        projects.emit(projectStorage.toSet())
    }
}

class RedisProjectStorage(
    uri: String,
    prefix: String,
) : ProjectStorage {

    private val redis = RedisConnection(uri, prefix)

    override val projects = MutableSharedFlow<Set<String>>(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    override suspend fun getProjects(): Set<String> {
        return redis.smembers(RedisKey.Projects) ?: emptySet()
    }

    override suspend fun putProject(projectId: String) {
        redis.sadd(RedisKey.Projects, setOf(projectId))
        projects.emit(getProjects())
    }

    override suspend fun removeProject(projectId: String) {
        redis.srem(RedisKey.Projects, projectId)
        projects.emit(getProjects())
    }
}
