package io.bordo.ddpclient.utils

/**
 * Marks a test that can only run on the JVM host.
 *
 * Ktor's `testApplication` has no websocket engine on Kotlin/Native -- the server side throws
 * `NotImplementedError`, so every test built on [testServer] fails there for reasons that have
 * nothing to do with this library. Those tests are marked with this annotation so the native run
 * reports them as *skipped* rather than passed; anything that does not need a live server (the
 * in-memory database, the message parser, `MeteorRandom`, `ConcurrentDecodeStressTest`, and the
 * connection tests that drive a mock/throwing engine) still runs on native, which is where it
 * matters most.
 *
 * Drop this once ktor ships native websocket support in its test host.
 */
expect annotation class IgnoreNative()
