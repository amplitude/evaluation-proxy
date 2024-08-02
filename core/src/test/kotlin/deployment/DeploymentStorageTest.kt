package deployment

import test.InMemoryRedis
import com.amplitude.deployment.DeploymentStorage
import com.amplitude.deployment.InMemoryDeploymentStorage
import com.amplitude.deployment.RedisDeploymentStorage
import test.deployment
import test.flag
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import kotlin.test.assertNull

class DeploymentStorageTest {

    private val redis = InMemoryRedis()

    @Test
    fun `test in memory`(): Unit = runBlocking {
        test(InMemoryDeploymentStorage())
    }

    @Test
    fun `test redis`(): Unit = runBlocking {
        test(RedisDeploymentStorage("amplitude", "12345", redis, redis))
    }

    private fun test(storage: DeploymentStorage): Unit = runBlocking {
        val deploymentA = deployment("a", "1")
        val deploymentB = deployment("b", "2")

        // get deployment, null
        var deployment = storage.getDeployment(deploymentA.key)
        assertNull(deployment)
        // get deployments, empty
        var deployments = storage.getDeployments()
        assertEquals(0, deployments.size)
        // put, get deployment, deployment
        storage.putDeployment(deploymentA)
        deployment = storage.getDeployment(deploymentA.key)
        assertEquals(deploymentA, deployment)
        // put, get deployments, deployments
        storage.putDeployment(deploymentB)
        deployments = storage.getDeployments()
        assertEquals(
            mapOf(deploymentA.key to deploymentA, deploymentB.key to deploymentB),
            deployments
        )

        val flag1 = flag("1")
        val flag2 = flag("2")

        // get flag, null
        var flag = storage.getFlag(deploymentA.key, flag1.key)
        assertNull(flag)
        // get all flags, empty
        var flags = storage.getAllFlags(deploymentA.key)
        assertEquals(0, flags.size)
        // put, get flag, flag
        storage.putFlag(deploymentA.key, flag1)
        flag = storage.getFlag(deploymentA.key, flag1.key)
        assertEquals(flag1, flag)
        // put, get all flags, flags
        storage.putFlag(deploymentA.key, flag2)
        flags = storage.getAllFlags(deploymentA.key)
        assertEquals(mapOf(flag1.key to flag1, flag2.key to flag2), flags)
        // remove, get removed, null
        storage.removeFlag(deploymentA.key, flag1.key)
        flag = storage.getFlag(deploymentA.key, flag1.key)
        assertNull(flag)
        // get other, other
        flag = storage.getFlag(deploymentA.key, flag2.key)
        assertEquals(flag2, flag)
        // get all flags, other
        flags = storage.getAllFlags(deploymentA.key)
        assertEquals(mapOf(flag2.key to flag2), flags)
        // remove all flags, get, null
        storage.removeAllFlags(deploymentA.key)
        // get all flags, empty
        flags = storage.getAllFlags(deploymentA.key)
        assertEquals(0, flags.size)

        // put flags
        storage.putFlag(deploymentA.key, flag1)
        storage.putFlag(deploymentA.key, flag2)
        flags = storage.getAllFlags(deploymentA.key)
        assertEquals(mapOf(flag1.key to flag1, flag2.key to flag2), flags)

        // remove deployment, get removed, null
        storage.removeDeployment(deploymentA.key)
        // get flag, null
        flag = storage.getFlag(deploymentA.key, flag1.key)
        assertNull(flag)
        flag = storage.getFlag(deploymentA.key, flag2.key)
        assertNull(flag)
        // get all flags, empty
        flags = storage.getAllFlags(deploymentA.key)
        assertEquals(0, flags.size)
        // get other, deployment
        deployment = storage.getDeployment(deploymentB.key)
        assertEquals(deploymentB, deployment)
        // get deployments, other
        deployments = storage.getDeployments()
        assertEquals(mapOf(deploymentB.key to deploymentB), deployments)
        // get all flags, empty
        flags = storage.getAllFlags(deploymentB.key)
        assertEquals(0, flags.size)
        // remove, get deployments, empty
        storage.removeDeployment(deploymentB.key)
        deployments = storage.getDeployments()
        assertEquals(0, deployments.size)
    }
}
