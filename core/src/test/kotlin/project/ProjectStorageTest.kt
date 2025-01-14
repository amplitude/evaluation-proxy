package project

import com.amplitude.project.InMemoryProjectStorage
import com.amplitude.project.ProjectStorage
import com.amplitude.project.RedisProjectStorage
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import test.InMemoryRedis
import kotlin.test.Test

class ProjectStorageTest {
    @Test
    fun `test in memory`(): Unit =
        runBlocking {
            test(InMemoryProjectStorage())
        }

    @Test
    fun `test redis`(): Unit =
        runBlocking {
            test(RedisProjectStorage("amplitude", InMemoryRedis(), 1000))
        }

    private fun test(storage: ProjectStorage) =
        runBlocking {
            // get projects, empty
            var projects = storage.getProjects()
            assertEquals(0, projects.size)
            // put project, 1
            storage.putProject("1")
            // put project, 2
            storage.putProject("2")
            // get projects, 1 2
            projects = storage.getProjects()
            assertEquals(setOf("1", "2"), projects)
            // remove project 1, 2
            storage.removeProject("1")
            // get projects, 2
            projects = storage.getProjects()
            assertEquals(setOf("2"), projects)
            // remove project 2
            storage.removeProject("2")
            // get projects, empty
            projects = storage.getProjects()
            assertEquals(0, projects.size)
        }
}
