plugins {
    application
    id("io.ktor.plugin") version "2.2.4"
    kotlin("jvm") version "1.8.10"
    id("org.jetbrains.kotlin.plugin.serialization") version "1.8.0"
    id("org.jlleitschuh.gradle.ktlint") version "10.3.0"
}

application {
    mainClass.set("com.amplitude.ServerKt")
    val isDevelopment: Boolean = project.ext.has("development")
    applicationDefaultJvmArgs = listOf("-Dio.ktor.development=$isDevelopment")
}

repositories {
    mavenLocal()
    mavenCentral()
    maven { url = uri("https://maven.pkg.jetbrains.space/public/p/ktor/eap") }
}

// Defined in gradle.properties
val ktorVersion: String by project
val kotlinVersion: String by project
val logbackVersion: String by project
val prometheusVersion: String by project
val serializationVersion: String by project
val experimentSdkVersion: String by project
val experimentEvaluationVersion: String by project

dependencies {
    // implementation("com.amplitude:experiment-jvm-server:$experimentSdkVersion")
    implementation("com.amplitude:evaluation-core:$experimentEvaluationVersion")
    implementation("com.amplitude:evaluation-serialization:$experimentEvaluationVersion")

    implementation("io.ktor:ktor-server-call-logging-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-core-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-metrics-micrometer-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation-jvm:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-netty-jvm:$ktorVersion")
    implementation("io.ktor:ktor-client-okhttp:$ktorVersion")

    implementation("io.micrometer:micrometer-registry-prometheus:$prometheusVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")

    testImplementation("io.ktor:ktor-server-tests-jvm:$ktorVersion")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:$kotlinVersion")
}
