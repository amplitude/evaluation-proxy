package com.amplitude

import kotlin.test.*
import io.ktor.server.testing.*

class ApplicationTest {
    @Test
    fun testRoot() = testApplication {
        // val experiment = Experiment.initializeLocal()
        // application {
        //     configureRouting()
        // }
        // client.get("/").apply {
        //     assertEquals(HttpStatusCode.OK, status)
        //     // assertEquals("Hello World!", bodyAsText())
        // }
    }
}
