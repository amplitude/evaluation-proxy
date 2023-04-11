package com.amplitude.experiment.assignment

import com.amplitude.assignment.Assignment
import com.amplitude.assignment.InMemoryAssignmentFilter
import com.amplitude.experiment.evaluation.SkylabUser
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test

class AssignmentFilterTest {

    @Test
    fun `test single assignment`() = runBlocking {
        val filter = InMemoryAssignmentFilter(100)
        val assignment = Assignment(
            SkylabUser(userId = "user"),
            mapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        Assert.assertTrue(filter.shouldTrack(assignment))
    }

    @Test
    fun `test duplicate assignments`() = runBlocking {
        val filter = InMemoryAssignmentFilter(100)
        val assignment1 = Assignment(
            SkylabUser(userId = "user"),
            mapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        filter.shouldTrack(assignment1)
        val assignment2 = Assignment(
            SkylabUser(userId = "user"),
            mapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        Assert.assertFalse(filter.shouldTrack(assignment2))
    }

    @Test
    fun `test same user different results`() = runBlocking {
        val filter = InMemoryAssignmentFilter(100)
        val assignment1 = Assignment(
            SkylabUser(userId = "user"),
            mapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        Assert.assertTrue(filter.shouldTrack(assignment1))
        val assignment2 = Assignment(
            SkylabUser(userId = "user"),
            mapOf(
                "flag-key-1" to flagResult("control"),
                "flag-key-2" to flagResult("on")
            )
        )
        Assert.assertTrue(filter.shouldTrack(assignment2))
    }

    @Test
    fun `test same results for different users`() = runBlocking {
        val filter = InMemoryAssignmentFilter(100)
        val assignment1 = Assignment(
            SkylabUser(userId = "user"),
            mapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        Assert.assertTrue(filter.shouldTrack(assignment1))
        val assignment2 = Assignment(
            SkylabUser(userId = "different user"),
            mapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        Assert.assertTrue(filter.shouldTrack(assignment2))
    }

    @Test
    fun `test empty results`() = runBlocking {
        val filter = InMemoryAssignmentFilter(100)
        val assignment1 = Assignment(
            SkylabUser(userId = "user"),
            mapOf()
        )
        Assert.assertTrue(filter.shouldTrack(assignment1))
        val assignment2 = Assignment(
            SkylabUser(userId = "user"),
            mapOf()
        )
        Assert.assertFalse(filter.shouldTrack(assignment2))
        val assignment3 = Assignment(
            SkylabUser(userId = "different user"),
            mapOf()
        )
        Assert.assertTrue(filter.shouldTrack(assignment3))
    }

    @Test
    fun `test duplicate assignments with different result ordering`() = runBlocking {
        val filter = InMemoryAssignmentFilter(100)
        val assignment1 = Assignment(
            SkylabUser(userId = "user"),
            linkedMapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        Assert.assertTrue(filter.shouldTrack(assignment1))
        val assignment2 = Assignment(
            SkylabUser(userId = "user"),
            linkedMapOf(
                "flag-key-2" to flagResult("control"),
                "flag-key-1" to flagResult("on")
            )
        )
        Assert.assertFalse(filter.shouldTrack(assignment2))
    }

    @Test
    fun `test lru replacement`() = runBlocking {
        val filter = InMemoryAssignmentFilter(2)
        val assignment1 = Assignment(
            SkylabUser(userId = "user1"),
            mapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        Assert.assertTrue(filter.shouldTrack(assignment1))
        val assignment2 = Assignment(
            SkylabUser(userId = "user2"),
            mapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        Assert.assertTrue(filter.shouldTrack(assignment2))
        val assignment3 = Assignment(
            SkylabUser(userId = "user3"),
            mapOf(
                "flag-key-1" to flagResult("on"),
                "flag-key-2" to flagResult("control")
            )
        )
        Assert.assertTrue(filter.shouldTrack(assignment3))
        Assert.assertTrue(filter.shouldTrack(assignment1))
    }
}
