package io.bordo.ddpclient.util

import kotlin.reflect.KClass

/**
 * Created by Osman Saral on 5.04.2024
 */

actual val platformExceptions: Set<KClass<out Exception>>
    get() = setOf(
        java.util.concurrent.CancellationException::class,
        // On Android the injected ktor engine is OkHttp. A rejected WebSocket upgrade
        // (server replies with a non-101 status, e.g. "400 Bad Request") surfaces as
        // OkHttp's java.net.ProtocolException ("Expected HTTP 101 response but was ...")
        // rather than ktor's WebSocketException. ProtocolException extends IOException.
        //
        // We treat the whole java.io.IOException family as recoverable connection failures
        // so the DDP flow does a graceful disconnect + bounded retry instead of crashing.
        // IOException is the right umbrella: every OkHttp/JVM transport failure that can
        // legitimately happen at runtime is an IOException subtype — ProtocolException
        // (failed upgrade), EOFException (truncated/closed stream), SocketException &
        // SocketTimeoutException (connection reset/timeout), UnknownHostException (DNS),
        // SSLException (TLS) — all transient/environmental and safe to retry.
        //
        // It does NOT mask genuine programming bugs: those surface as RuntimeException
        // subtypes (NullPointerException, IllegalStateException, IllegalArgumentException),
        // which are not IOExceptions and so still propagate uncaught.
        java.io.IOException::class,
    )
