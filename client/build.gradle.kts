import com.android.build.api.dsl.KotlinMultiplatformAndroidLibraryExtension
import org.gradle.api.plugins.ExtensionAware

plugins {
    // Plugins are requested WITHOUT versions on purpose. These sources are also built in place
    // by the app they were extracted from, where Kotlin and AGP are already on the build
    // classpath via convention plugins -- a versioned request there fails with "already on the
    // classpath with an unknown version". Each build's settings.gradle.kts supplies versions.
    id("org.jetbrains.kotlin.multiplatform")
    id("com.android.kotlin.multiplatform.library")
    id("org.jetbrains.kotlin.plugin.serialization")
    id("com.vanniktech.maven.publish.base")
}

kotlin {
    applyDefaultHierarchyTemplate()
    jvmToolchain(17)

    // AGP 9.3 removed the top-level androidLibrary {} accessor; the extension is registered on the
    // Kotlin extension, so reach it by type.
    (this as ExtensionAware).extensions
        .getByType(KotlinMultiplatformAndroidLibraryExtension::class.java)
        .apply {
            namespace = "io.bordo.ddpclient"
            compileSdk = 37
            minSdk = 26
            // The classic androidTarget() gave commonTest a JVM run for free; the KMP Android
            // plugin does not. Without this, `allTests` silently drops to iOS-only.
            withHostTest {}
        }

    iosArm64()
    iosSimulatorArm64()
    // Intel-Mac simulators, and a prerequisite for an XCFramework later. Free: the default
    // hierarchy routes it through the existing iosMain actuals.
    iosX64()

    targets.configureEach {
        compilations.configureEach {
            compileTaskProvider.configure {
                compilerOptions {
                    optIn.addAll(
                        "kotlin.time.ExperimentalTime",
                        "kotlinx.cinterop.ExperimentalForeignApi",
                        "kotlinx.cinterop.BetaInteropApi",
                    )
                    freeCompilerArgs.add("-Xexpect-actual-classes")
                }
            }
        }
    }

    sourceSets {
        commonMain.dependencies {
            api(project(":ddpclient-ejson")) // plain path: portable across both repos
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.ktor.client.core)
            implementation(libs.ktor.client.serialization)
            implementation(libs.ktor.client.websockets)
            implementation(libs.ktor.serialization)
            implementation(libs.ktor.serialization.kotlinx.json)
            implementation(libs.stately.concurrent.collections)
            implementation(libs.kermit) //TODO: use a logger interface instead.
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation(libs.kotlinx.coroutines.test)
            // CIO is no longer shipped in the app, but the no-arg HttpClient() in
            // testHttpClient still needs an auto-selectable engine on the test classpath.
            implementation(libs.ktor.client.cio)
            implementation(libs.ktor.client.mock)
            implementation(libs.ktor.server.test.host)
            implementation(libs.ktor.server.websockets)
            implementation(libs.ktor.test.dispatcher)
            implementation(libs.slf4j.simple)
            implementation(libs.turbine)
        }
        androidMain.dependencies {
            implementation(libs.ktor.client.okhttp)
        }
        getByName("androidHostTest").dependencies {
            implementation(kotlin("test"))
        }
        iosMain.dependencies {
            implementation(libs.ktor.client.darwin)
        }
    }
}
