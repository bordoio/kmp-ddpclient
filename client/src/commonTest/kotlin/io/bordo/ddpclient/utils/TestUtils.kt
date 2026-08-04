package io.bordo.ddpclient.utils

import app.cash.turbine.ReceiveTurbine
import app.cash.turbine.test
import io.bordo.ddpclient.ddpclient.DDPClient
import io.bordo.ddpclient.ddpclient.DDPClientConfig
import io.bordo.ddpclient.ddpclient.Incoming
import io.bordo.ddpclient.ddpclient.Outgoing
import io.bordo.ddpclient.ddpclient.defaultJson
import io.ktor.server.application.install
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.receiveDeserialized
import io.ktor.server.websocket.sendSerialized
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.close
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlin.test.assertIs
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Created by Osman Saral on 25.03.2023
 */

const val SESSION_ID = "123"

/**
 * Ktor 3's [testApplication] runs the body inside `runTest`, so a [TestCoroutineScheduler] is in
 * context. Turbine sees it and collects on an `UnconfinedTestDispatcher`, which puts the DDP client's
 * `withTimeoutOrNull` calls on *virtual* time -- they fire instantly while the real websocket IO is
 * still in flight. The scheduler can't be removed with `withContext` (contexts merge), so run the body
 * in a detached scope that never had it. Ktor 2's `testApplication` used `runBlocking`, hence no such
 * issue before the 3.x upgrade.
 */
fun ddpTestApplication(block: suspend ApplicationTestBuilder.() -> Unit) = testApplication {
    CoroutineScope(Dispatchers.Default).async { block() }.await()
}

inline fun ApplicationTestBuilder.testServer(crossinline block: suspend DefaultWebSocketServerSession.() -> Unit) {
    externalServices {
        hosts("wss://test") {
            install(io.ktor.server.websocket.WebSockets) {
                contentConverter = DDPServerMessageConverter(defaultJson)
            }
            routing {
                route("{...}") {
                    webSocket {
                        sendSerialized(Incoming.Open)
                        receive {
//                            val sessionId = if (it is Outgoing.Connect && it.session != null) it.session else SESSION_ID
                            assertIs<Outgoing.Connect>(it)
                            sendSerialized(Incoming.Connected(SESSION_ID))
                        }
                        block()
                        holdOpen()
                    }
                }
            }
        }
    }
}

inline fun ApplicationTestBuilder.testDisconnectingServer(crossinline block: suspend DefaultWebSocketServerSession.() -> Unit) {
    var sessionId: String? = null

    externalServices {
        hosts("wss://test") {
            install(io.ktor.server.websocket.WebSockets) {
                contentConverter = DDPServerMessageConverter(defaultJson)
            }
            routing {
                route("{...}") {
                    webSocket {
                        sendSerialized(Incoming.Open)
                        receive {
                            assertIs<Outgoing.Connect>(it)
                            sessionId = it.session
                            if (sessionId == null) {
                                sendSerialized(Incoming.Connected(SESSION_ID))
                            } else {
                                close(CloseReason(CloseReason.Codes.NORMAL, "closed"))
                            }
                        }
                        block()
                    }
                }
            }
        }
    }
}

/**
 * Keeps the websocket session alive until the client (or `testApplication` teardown) closes it.
 *
 * Returning from ktor's `webSocket { }` handler closes the session immediately, which races
 * whatever the client still has in flight -- a retry after `tokenRefresher`, a `c[1000]` close
 * frame the client hasn't parsed yet, an assertion on `connectionState`. The client then reports
 * `Incoming.Exception` / `MethodState.Exception` / `Disconnected` instead of the expected result.
 * Tests that want the server to close say so explicitly (`close(CloseReason(...))` or a raw
 * `c[1000,"Normal closure"]` frame), so staying open by default is also the truthful behaviour.
 *
 * Draining `incoming` (rather than `awaitCancellation()`) means this returns as soon as the
 * client disconnects, so nothing hangs waiting for the test framework to tear the server down.
 */
suspend fun DefaultWebSocketServerSession.holdOpen() {
    try {
        incoming.consumeEach { /* drain and discard */ }
    } catch (_: Exception) {
        // Channel closed with a cause -- the client went away, which is the point.
    }
}

suspend inline fun DefaultWebSocketServerSession.receive(times: Int = 1, crossinline block: suspend DefaultWebSocketServerSession.(receivedMessage: Outgoing) -> Unit) {
    repeat(times) {
        try {
            val outgoing = receiveDeserialized<Outgoing>()
            block(outgoing)
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}

/**
 * The timeout is a hang guard, not an assertion about speed, so it is generous. At 2.5s the tests
 * that drive a full reconnect (close -> restart -> token refresh -> resend, with the default 500ms
 * retryDelay) flaked roughly one run in three on a loaded machine, and CI runners are slower still.
 * A genuine hang still fails, just later.
 */
suspend inline fun Flow<Incoming>.testAfterConnected(
    timeout: Duration? = 10.seconds,
    name: String? = null,
    crossinline validate: suspend ReceiveTurbine<Incoming>.() -> Unit,
) = test(timeout, name) {
    awaitItem().also { println("received1: $it") }
    awaitItem().also { println("received2: $it") }
    validate()
}

/**
 * Consumes emissions until the collection settles on [expected].
 *
 * Replaces `delay(100); expectMostRecentItem()`, which asserted against whatever had arrived within
 * a fixed sleep -- fine on an idle machine, flaky under load, and silently timing-dependent either
 * way. Here the turbine timeout is the only failure mode, and it reports what was actually seen.
 */
suspend fun <T> ReceiveTurbine<T>.awaitItemEqualTo(expected: T) {
    var last: T? = null
    try {
        while (true) {
            last = awaitItem()
            if (last == expected) return
        }
    } catch (e: AssertionError) {
        throw AssertionError("Never settled on <$expected>; last item was <$last>", e)
    }
}

fun ApplicationTestBuilder.TestDDPClient(
    block: DDPClientConfig.() -> Unit = {},
): DDPClient = DDPClient("test", clientFactory = {
    createClient(it)
}, {
    retryConnection = false
    pingPongEnabled = false
    block()
})
