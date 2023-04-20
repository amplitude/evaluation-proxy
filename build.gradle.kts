plugins {
    kotlin("jvm") version "1.8.10"
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
}

allprojects {
    group = "com.amplitude"

    repositories {
        mavenCentral()
        maven { url = uri("https://maven.pkg.jetbrains.space/public/p/ktor/eap") }
    }
}

nexusPublishing {
    repositories {
        sonatype {
            stagingProfileId.set(System.getenv("SONATYPE_STAGING_PROFILE_ID"))
            username.set(System.getenv("SONATYPE_USERNAME"))
            password.set(System.getenv("SONATYPE_PASSWORD"))
        }
    }
}
