// The build definition for this repo.
//
// These sources are also consumed in place by the private app they were extracted from, whose
// own settings.gradle.kts declares the same project paths -- so this file is ignored there and
// the module build files stay identical in both builds.

pluginManagement {
    // The module build files request plugins without versions so they work unchanged in both
    // repos; the monorepo supplies these from build-logic's classpath, this build from here.
    plugins {
        id("org.jetbrains.kotlin.multiplatform") version "2.4.0"
        id("org.jetbrains.kotlin.plugin.serialization") version "2.4.0"
        id("com.android.kotlin.multiplatform.library") version "9.3.0"
        id("com.vanniktech.maven.publish.base") version "0.33.0"
    }

    repositories {
        google {
            mavenContent {
                includeGroupAndSubgroups("androidx")
                includeGroupAndSubgroups("com.android")
                includeGroupAndSubgroups("com.google")
            }
        }
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        google {
            mavenContent {
                includeGroupAndSubgroups("androidx")
                includeGroupAndSubgroups("com.android")
                includeGroupAndSubgroups("com.google")
            }
        }
        mavenCentral()
    }
}

rootProject.name = "ddpclient-root"

// Project paths, not directory names: the path is the Maven artifactId and the composite-build
// substitution key, and it must be identical in the monorepo so client/build.gradle.kts can
// reference project(":ddpclient-ejson") in both builds.
include(":ddpclient", ":ddpclient-ejson")
project(":ddpclient").projectDir = file("client")
project(":ddpclient-ejson").projectDir = file("ejson")
