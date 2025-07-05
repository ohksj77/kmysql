plugins {
    kotlin("jvm") version "2.1.20"
    application
}

group = "ksj"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("kmysql.server.StartServer")
}
