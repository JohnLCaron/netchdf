 plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    implementation(project(":core"))

    implementation(libs.lzf)
    implementation(libs.lz4)
    implementation(libs.oshai.logging)
    implementation(libs.logback.classic)

    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.register<Jar>("uberJar") {
    archiveClassifier = "uber"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    archiveBaseName.set("netchdf")

    manifest {
        attributes("Main-Class" to "com.sunya.netchdf.cli.ncdump")
    }

    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    })
}

project.tasks["compileJava"].dependsOn(":core:allMetadataJar")
