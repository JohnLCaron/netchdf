plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    api(project(":core"))
    // implementation(project(":cli"))

    implementation(libs.jhdf)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.okio)
    implementation(libs.fleeksoft)
    implementation(libs.oshai.logging)
    implementation(libs.logback.classic)

    testImplementation(kotlin("test"))
    testImplementation(libs.bundles.jvmtest)
}

kotlin {
    jvmToolchain(21)

    compilerOptions {
        optIn.add("kotlin.RequiresOptIn")
    }
}

tasks {
    val ENABLE_PREVIEW = "--enable-preview"
    withType<JavaCompile>() {
        options.compilerArgs.add(ENABLE_PREVIEW)
        // Optionally we can show which preview feature we use.
        // options.compilerArgs.add("-Xlint:preview")
        // Explicitly setting compiler option --release
        // is needed when we wouldn't set the
        // sourceCompatiblity and targetCompatibility
        // properties of the Java plugin extension.
        options.release.set(21)
    }
    withType<Test>().all {
        useJUnitPlatform()
        jvmArgs("--enable-preview")
        minHeapSize = "512m"
        maxHeapSize = "4g"
        // https://www.jvt.me/posts/2021/03/11/gradle-speed-parallel/
        // Configuration parameters to execute top-level classes in parallel but methods in same thread
        // https://junit.org/junit5/docs/current/user-guide/#writing-tests-parallel-execution
        systemProperties["junit.jupiter.execution.parallel.enabled"] = "true"
        systemProperties["junit.jupiter.execution.parallel.mode.default"] = "concurrent"
        systemProperties["junit.jupiter.execution.parallel.mode.classes.default"] = "concurrent"

        // https://kantis.github.io/posts/Faster-Kotest-startup/
        systemProperty("kotest.framework.discovery.jar.scan.disable", "true")
        systemProperty("kotest.framework.classpath.scanning.config.disable", "true")
        systemProperty("kotest.framework.classpath.scanning.autoscan.disable", "true")
    }
    withType<JavaExec>().all {
        jvmArgs("--enable-preview")
    }
}

// Declare an explicit dependency on ':core:allMetadataJar' from ':testclibs:compileJava' using Task#dependsOn
// project.tasks["compileJava"].dependsOn(":core:allMetadataJar")