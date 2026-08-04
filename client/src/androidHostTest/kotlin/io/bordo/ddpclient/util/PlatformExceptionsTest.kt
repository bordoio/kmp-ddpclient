package io.bordo.ddpclient.util

import java.io.EOFException
import java.io.IOException
import java.net.ProtocolException
import java.net.SocketException
import java.net.SocketTimeoutException
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Guards the crash fix for the DDP WebSocket-upgrade ProtocolException
 * ("Expected HTTP 101 response but was '400 Bad Request'") seen on Android 2.14.0.
 *
 * On Android the ktor engine is OkHttp, so a rejected upgrade surfaces as a
 * java.net.ProtocolException (an IOException) instead of ktor's WebSocketException.
 * The DDP flow only treats exceptions in [platformExceptions] (+ the common defaults)
 * as recoverable; anything else is re-thrown and crashes the app. These tests assert the
 * IOException umbrella now classifies the upgrade/transport failures as recoverable while
 * still letting genuine programming bugs propagate.
 */
class PlatformExceptionsTest {

    private fun isHandled(t: Throwable) = platformExceptions.any { it.isInstance(t) }

    @Test
    fun `the original crash exception is now treated as recoverable`() {
        val crash = ProtocolException("Expected HTTP 101 response but was '400 Bad Request'")
        assertTrue(isHandled(crash), "ProtocolException from a failed WS upgrade must be handled, not crash")
    }

    @Test
    fun `transient transport failures are treated as recoverable`() {
        listOf(
            ProtocolException("upgrade rejected"),
            EOFException("stream closed mid-handshake"),
            SocketException("Connection reset"),
            SocketTimeoutException("timeout"),
            IOException("generic I/O failure"),
        ).forEach { t ->
            assertTrue(isHandled(t), "${t::class.simpleName} should be recoverable")
        }
    }

    @Test
    fun `genuine programming bugs still propagate`() {
        listOf(
            NullPointerException("npe"),
            IllegalStateException("ise"),
            IllegalArgumentException("iae"),
        ).forEach { t ->
            assertFalse(isHandled(t), "${t::class.simpleName} must NOT be swallowed by the transport umbrella")
        }
    }
}
