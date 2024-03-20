
group = "ru.ifmo.pds"
version = "1.0-SNAPSHOT"

plugins {
    kotlin("jvm") version "1.9.22"
    kotlin("plugin.serialization") version "1.9.22"
    id("org.jetbrains.kotlinx.kover") version "0.7.6"
}

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
    testImplementation(kotlin("test-junit"))
}

sourceSets["main"].java.setSrcDirs(listOf("src"))
sourceSets["test"].java.setSrcDirs(listOf("test"))

val processId = project.properties["processId"] as? String ?: "1"
val implName = project.properties["implName"] as? String ?: "ProcessImpl"

tasks {
    test {
        testLogging.showStandardStreams = true
        systemProperty("implName", implName)
        filter { excludeTestsMatching("*Distributed*") }
    }

    val distributedTest by registering(Test::class) {
        group = "verification"
        testLogging.showStandardStreams = true
        filter { includeTestsMatching("*Distributed*") }
    }

    check {
        dependsOn(distributedTest)
    }

    register<JavaExec>("node") {
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("raft.system.NodeKt")
        args = listOf(processId, implName)
        standardInput = System.`in`
    }

    register<JavaExec>("system") {
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("raft.system.SystemKt")
        args = listOf(implName)
        standardInput = System.`in`
    }
}

