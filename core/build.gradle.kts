import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm")
    kotlin("plugin.serialization") version "2.0.0"
    id("com.vanniktech.maven.publish") version "0.34.0"
    id("org.jlleitschuh.gradle.ktlint") version "12.1.1"
    id("org.jetbrains.dokka") version "1.9.20"
}

repositories {
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
val coroutinesVersion: String by project
val serializationVersion: String by project
val experimentEvaluationVersion: String by project
val amplitudeAnalytics: String by project
val amplitudeAnalyticsJson: String by project
val lettuce: String by project
val kaml: String by project
val mockk: String by project

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    implementation("com.amplitude:evaluation-core:$experimentEvaluationVersion")
    implementation("com.amplitude:java-sdk:$amplitudeAnalytics")
    implementation("org.json:json:$amplitudeAnalyticsJson")
    implementation("io.lettuce:lettuce-core:$lettuce")
    implementation("io.ktor:ktor-client-okhttp:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:$ktorVersion")
    implementation("com.squareup.okio:okio:3.9.0")
    implementation("com.squareup.moshi:moshi:1.15.1")
    implementation("com.charleskorn.kaml:kaml:$kaml")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:$kotlinVersion")
    testImplementation("io.mockk:mockk:$mockk")
    testImplementation("io.ktor:ktor-client-mock:$ktorVersion")
}

// Publishing

group = "com.amplitude"
version = "0.11.1"

mavenPublishing {
    coordinates(
        group as String?,
        "evaluation-proxy-core",
        version as String?,
    )

    configure(
        KotlinJvm(
            javadocJar = JavadocJar.Dokka("dokkaHtml"),
            sourcesJar = true,
        ),
    )

    pom {
        name.set("Amplitude Evaluation Proxy")
        description.set("Core package for Amplitude's evaluation proxy.")
        url.set("https://github.com/amplitude/evaluation-proxy")
        licenses {
            license {
                name.set("MIT")
                url.set("https://opensource.org/licenses/MIT")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("amplitude")
                name.set("Amplitude")
                email.set("dev@amplitude.com")
            }
        }
        scm {
            url.set("https://github.com/amplitude/evaluation-proxy")
        }
    }

    publishToMavenCentral()
    signAllPublications()
}
