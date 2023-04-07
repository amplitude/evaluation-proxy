import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.10"
    id("org.jetbrains.kotlin.plugin.serialization") version "1.8.0"
}

repositories {
    mavenLocal()
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks {
    withType<KotlinCompile> { kotlinOptions { jvmTarget = "17" } }
}

// Defined in gradle.properties
val kotlinVersion: String by project
val ktorVersion: String by project
val experimentEvaluationVersion: String by project
val amplitudeAnalytics: String by project
val amplitudeAnalyticsJson: String by project
val lettuce: String by project
val apacheCommons: String by project

dependencies {
    implementation("com.amplitude:evaluation-core:$experimentEvaluationVersion")
    implementation("com.amplitude:evaluation-serialization:$experimentEvaluationVersion")
    implementation("com.amplitude:java-sdk:$amplitudeAnalytics")
    implementation("org.json:json:$amplitudeAnalyticsJson")
    implementation("io.lettuce:lettuce-core:$lettuce")
    implementation("io.ktor:ktor-client-okhttp:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:$ktorVersion")
    implementation("org.apache.commons:commons-csv:$apacheCommons")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:$kotlinVersion")
}
