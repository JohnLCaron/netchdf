// copied from kobweb
plugins {
    alias(libs.plugins.kotlin.multiplatform) apply  false
    alias(libs.plugins.kotlin.jvm) apply false
    `kotlin-dsl` apply false
    alias(libs.plugins.kotlinx.serialization) apply false
}

group = "com.sunya.netchdf"
version = libs.versions.netchdf.get()

subprojects {
    repositories {
        mavenCentral()
        google()
    }
}

// To upgrade gradle, bump the version below and run the wrapper task:
//     ./gradlew wrapper
//
// Note: you should run the task twice when upgrading - once to update
//       the wrapper files, and the second time to actually download
//       and use the new version.
tasks.wrapper {
    distributionType = Wrapper.DistributionType.BIN
    gradleVersion = "9.0.0"
}


tasks {
    withType<Test>().all {
        useJUnitPlatform()
        minHeapSize = "512m"
        maxHeapSize = "8g"
        jvmArgs = listOf("-Xss128m")

        // Make tests run in parallel
        // More info: https://www.jvt.me/posts/2021/03/11/gradle-speed-parallel/
        systemProperties["junit.jupiter.execution.parallel.enabled"] = "true"
        systemProperties["junit.jupiter.execution.parallel.mode.default"] = "concurrent"
        systemProperties["junit.jupiter.execution.parallel.mode.classes.default"] = "concurrent"

        systemProperty("kotest.framework.discovery.jar.scan.disable", "true")
        systemProperty("kotest.framework.classpath.scanning.config.disable", "true")
        systemProperty("kotest.framework.classpath.scanning.autoscan.disable", "true")
    }
}