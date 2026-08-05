// Aggregator for the standalone ddpclient repo. Inert in the monorepo (see settings.gradle.kts).
//
// The shared POM lives here rather than being repeated in each module. Coordinates and the
// publishing target are set per-module, since only the artifactId differs.

import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinMultiplatform
import com.vanniktech.maven.publish.MavenPublishBaseExtension

plugins {
    // All `apply false`: the root project builds and publishes nothing. They are declared here so
    // their classes land on this build script's classpath — needed for the imports and the
    // subprojects{} block below, and because the publish plugin refuses to configure a Kotlin
    // project unless it can see the Kotlin plugin classes from the same classloader.
    id("org.jetbrains.kotlin.multiplatform") apply false
    id("com.android.kotlin.multiplatform.library") apply false
    id("org.jetbrains.kotlin.plugin.serialization") apply false
    id("com.vanniktech.maven.publish.base") apply false
}

subprojects {
    // Set on the project, not just on the publication. The publish plugin only needs
    // coordinates(), but Gradle matches an included build's projects to external dependencies by
    // project.group + project.name -- so without this, a consumer using includeBuild() on this
    // repo gets "Could not find io.bordo:ddpclient" and falls through to Maven Central.
    group = providers.gradleProperty("GROUP").get()
    version = providers.gradleProperty("VERSION_NAME").get()

    pluginManager.withPlugin("com.vanniktech.maven.publish.base") {
        extensions.configure<MavenPublishBaseExtension> {
            // JavadocJar.Empty() satisfies Central's javadoc requirement without wiring Dokka --
            // the README is what people actually read. Sources are published by the KMP plugin.
            configure(KotlinMultiplatform(javadocJar = JavadocJar.Empty()))

            coordinates(
                groupId = project.property("GROUP").toString(),
                artifactId = project.name,
                version = project.property("VERSION_NAME").toString(),
            )

            publishToMavenCentral()

            // Central requires signatures, but signing unconditionally breaks
            // publishToMavenLocal for anyone without a GPG key -- which is how you smoke-test a
            // build before releasing. CI provides the key as ORG_GRADLE_PROJECT_signingInMemoryKey.
            if (providers.gradleProperty("signingInMemoryKey").isPresent) {
                signAllPublications()
            }

            pom {
                name.set(project.name)
                description.set(
                    "A Kotlin Multiplatform client for Meteor's DDP protocol: websocket " +
                        "connection management, subscriptions with an in-memory minimongo mirror, " +
                        "and method calls as coroutine Flows."
                )
                inceptionYear.set("2023")
                url.set("https://github.com/bordoio/kmp-ddpclient")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        id.set("osrl")
                        name.set("Osman Saral")
                        url.set("https://github.com/osrl")
                    }
                }
                scm {
                    url.set("https://github.com/bordoio/kmp-ddpclient")
                    connection.set("scm:git:git://github.com/bordoio/kmp-ddpclient.git")
                    developerConnection.set("scm:git:ssh://git@github.com/bordoio/kmp-ddpclient.git")
                }
            }
        }
    }
}
