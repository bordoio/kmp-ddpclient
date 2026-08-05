import com.android.build.api.dsl.KotlinMultiplatformAndroidLibraryExtension
import org.gradle.api.plugins.ExtensionAware

plugins {
    // Unversioned for the same reason as ../client/build.gradle.kts.
    id("org.jetbrains.kotlin.multiplatform")
    id("com.android.kotlin.multiplatform.library")
    id("org.jetbrains.kotlin.plugin.serialization")
    id("com.vanniktech.maven.publish.base")
}

kotlin {
    applyDefaultHierarchyTemplate()
    jvmToolchain(17)

    (this as ExtensionAware).extensions
        .getByType(KotlinMultiplatformAndroidLibraryExtension::class.java)
        .apply {
            namespace = "io.bordo.ddpclient.ejson"
            compileSdk = 37
            minSdk = 26
            withHostTest {}
        }

    iosArm64()
    iosSimulatorArm64()
    iosX64()
    jvm()

    targets.configureEach {
        compilations.configureEach {
            compileTaskProvider.configure {
                compilerOptions {
                    optIn.add("kotlin.time.ExperimentalTime")
                    freeCompilerArgs.add("-Xexpect-actual-classes")
                }
            }
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlinx.serialization.json)
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
        }
    }
}
