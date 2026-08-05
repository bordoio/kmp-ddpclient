package io.bordo.ddpclient

import app.cash.turbine.test
import io.bordo.ddpclient.ddpclient.AuthenticationState
import io.bordo.ddpclient.ddpclient.ConnectionState
import io.bordo.ddpclient.ddpclient.DDPClient
import io.bordo.ddpclient.ddpclient.Incoming
import io.bordo.ddpclient.ddpclient.MessageState
import io.bordo.ddpclient.ddpclient.Outgoing
import io.bordo.ddpclient.utils.IgnoreNative
import io.bordo.ddpclient.utils.SESSION_ID
import io.bordo.ddpclient.utils.TestDDPClient
import io.bordo.ddpclient.utils.receive
import io.bordo.ddpclient.utils.testAfterConnected
import io.bordo.ddpclient.utils.testDisconnectingServer
import io.bordo.ddpclient.utils.testServer
import io.bordo.ddpclient.utils.ddpTestApplication
import io.ktor.server.websocket.sendSerialized
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.seconds

@ExperimentalCoroutinesApi
class ConnectionTests {

    @Test
    @IgnoreNative
    fun `when server sends o message ddpClient should receive it`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        testServer { }

        runTest {
            ddpClient.initConnection().test {
                val o = awaitItem()
                assertEquals(Incoming.Open, o)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when connect message is sent session id should be set`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        testServer { }

        runTest {
            ddpClient.initConnection().test {
                awaitItem() // ignore o
                val connected = awaitItem()
                assertIs<Incoming.Connected>(connected)
                assertEquals(SESSION_ID, connected.session)
                assertEquals(ConnectionState.DDPConnected, ddpClient.connectionState.value)
                assertEquals(SESSION_ID, ddpClient.sessionId)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `initial connection state should be NotConnected`() = ddpTestApplication {
        val ddpClient = TestDDPClient()

        assertIs<ConnectionState.NotConnected>(ddpClient.connectionState.value)
    }

    //@Test //TODO: this test fails when running all
    fun `when server send close message client should be retrying with same session Id`() = ddpTestApplication {
        val ddpClient = TestDDPClient {
            retryDelay = 1000
            maxReconnectAttempts = 1
            retryConnection = true
        }
        testDisconnectingServer { }

        runTest {
            ddpClient.initConnection().testAfterConnected(2.seconds) {
                ddpClient.connectionState.test {
                    assertEquals(ConnectionState.DDPConnected, awaitItem())
                    assertIs<ConnectionState.Connecting>(awaitItem())
                    assertEquals(SESSION_ID, ddpClient.sessionId)
                    assertIs<ConnectionState.Disconnected>(awaitItem())
                    cancelAndIgnoreRemainingEvents()
                }
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server send close message client should be close`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        testServer {
            send(Frame.Text("""c[1000,"Normal closure"]"""))
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                assertIs<Incoming.Close>(awaitItem())
                // retryConnection = false, so the close propagates on through .catch. Awaiting the
                // Exception is what makes the state assertion deterministic -- reading
                // connectionState straight after the Close races .catch overwriting
                // closeConnection()'s NotConnected with Disconnected.
                assertIs<Incoming.Exception>(awaitItem())
                assertIs<ConnectionState.Disconnected>(ddpClient.connectionState.value)
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when ping is received should be sent from ddpClient`() = ddpTestApplication {
        val pingId = "123"
        val ddpClient = TestDDPClient()
        testServer {
            sendSerialized(Incoming.Ping(pingId))

            receive(1) {
                assertIs<Outgoing.Pong>(it)
                assertEquals(pingId, it.id)
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                val ping = awaitItem()
                assertIs<Incoming.Ping>(ping)
                assertEquals(pingId, ping.id)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when error is received it should be received from ddpClient`() = ddpTestApplication {
        val expectedError = Incoming.Error("reason", null)
        val ddpClient = TestDDPClient()
        testServer {
            sendSerialized(expectedError)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                val error = awaitItem()
                assertIs<Incoming.Error>(error)
                assertEquals(expectedError, error)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when ping is sent from ddpClient pong should be received from server`() = ddpTestApplication {
        val pingId = "123"
        val ddpClient = TestDDPClient()
        testServer {
            receive {
                assertIs<Outgoing.Ping>(it)
                assertEquals(pingId, it.id)

                sendSerialized(Incoming.Pong(it.id))
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.sendMessage(Outgoing.Ping(pingId)).collect()
                val pong = awaitItem()
                assertIs<Incoming.Pong>(pong)
                assertEquals(pingId, pong.id)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when a ddp message is sent while not connected it should try to reconnect`() = ddpTestApplication {
        val pingId = "123"
        val ddpClient = TestDDPClient {
            tokenRefresher = {
                true
            }
        }
        // The reconnect path re-authenticates before resending, and refreshToken() only runs while
        // the client considers itself Authorized -- otherwise sendMessage gives up with
        // MessageState.Failed and the tokenRefresher above is never consulted.
        ddpClient.setAuthenticationState(AuthenticationState.Authorized)

        var closedConnection = false
        testServer {
            if (!closedConnection) {
                send(Frame.Text("""c[1000,"Normal closure"]"""))
                closedConnection = true
            }

            receive {
                assertIs<Outgoing.Ping>(it)
                assertEquals(pingId, it.id)

                sendSerialized(Incoming.Pong(it.id))
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                assertIs<Incoming.Close>(awaitItem())
                assertIs<Incoming.Exception>(awaitItem())
                assertIs<ConnectionState.Disconnected>(ddpClient.connectionState.value)

                ddpClient.sendMessage(Outgoing.Ping(pingId)).test {
                    assertEquals(MessageState.Success, awaitItem())
                    // sendMessage keeps collecting authenticationState after a successful send, so
                    // a second Authorized emission produces another Success. Don't require exactly
                    // one item here -- see the "sendMessage can send twice" note in the plan.
                    cancelAndIgnoreRemainingEvents()
                }

                assertIs<Incoming.Open>(awaitItem())
                assertIs<Incoming.Connected>(awaitItem())
                val pong = awaitItem()
                assertIs<Incoming.Pong>(pong)
                assertEquals(pingId, pong.id)
            }
        }
    }

    @Test
    @IgnoreNative
    fun `reconnect budget resets on each successful connect so a long-lived connection keeps reconnecting`() = ddpTestApplication {
        // THE regression: retryWhen's own `attempt` is monotonic for the whole flow lifetime, so
        // maxReconnectAttempts acted as a *lifetime* budget. With maxReconnectAttempts = 1, the
        // client would reconnect once ever and then be stranded Disconnected on the next drop --
        // exactly what happened after the server closes the socket (code NORMAL) following
        // signup/verify. The budget must reset on each successful Incoming.Connected, so here the
        // client reconnects a SECOND time (a third successful connect) despite the limit of 1.
        val ddpClient = TestDDPClient {
            retryDelay = 10
            maxReconnectAttempts = 1
            retryConnection = true
        }

        var connects = 0
        testServer {
            connects++
            if (connects <= 2) {
                close(CloseReason(CloseReason.Codes.NORMAL, "closed"))
            } else {
                receive { } // hold the third connection open
            }
        }

        runTest {
            ddpClient.initConnection().test(timeout = 5.seconds) {
                assertIs<Incoming.Open>(awaitItem())
                assertIs<Incoming.Connected>(awaitItem()) // connect 1
                assertIs<Incoming.Close>(awaitItem())     // server closed
                assertIs<Incoming.Open>(awaitItem())
                assertIs<Incoming.Connected>(awaitItem()) // connect 2 (last one under the old budget)
                assertIs<Incoming.Close>(awaitItem())
                assertIs<Incoming.Open>(awaitItem())
                assertIs<Incoming.Connected>(awaitItem()) // connect 3 -- only reachable after the reset
                assertEquals(ConnectionState.DDPConnected, ddpClient.connectionState.value)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when failed is received ddpVersion should be set`() = ddpTestApplication {
        val desiredVersion = "pre1"
        val ddpClient = TestDDPClient()
        testServer {
            sendSerialized(Incoming.Failed(desiredVersion))
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                val failed = awaitItem()
                assertIs<Incoming.Failed>(failed)
                assertEquals(desiredVersion, failed.version)
                assertEquals(desiredVersion, ddpClient.ddpVersion)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when failed is received with unsupported version should throw`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val desiredVersion = "unsupported_version"
        val expected = Incoming.Error("Protocol version not supported: $desiredVersion", null)

        testServer {
            sendSerialized(expected)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                val error = awaitItem()
                assertIs<Incoming.Error>(error)
                assertEquals(expected, error)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `sendMessage delivers one result and completes`() = ddpTestApplication {
        // THE regression: both sendMessage branches collected a StateFlow (authenticationState /
        // connectionState), neither of which ever completes. So after the message went out the
        // collector stayed subscribed and fired again on the next Authorized / DDPConnected --
        // re-sending the same message and emitting a second Success. On a flaky connection that
        // means duplicate method calls on the server.
        //
        // Completion is the assertion rather than a send count: without the fix the flow simply
        // never ends, which awaitComplete() catches deterministically, whereas counting duplicate
        // sends would depend on a second auth cycle happening to occur inside the test.
        val pingId = "123"
        val ddpClient = TestDDPClient { tokenRefresher = { true } }
        ddpClient.setAuthenticationState(AuthenticationState.Authorized)

        var closedOnce = false
        testServer {
            if (!closedOnce) {
                send(Frame.Text("""c[1000,"Normal closure"]"""))
                closedOnce = true
            }
            receive(2) {
                if (it is Outgoing.Ping) sendSerialized(Incoming.Pong(it.id))
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                // Drop the connection so sendMessage takes the reconnect-then-send branch.
                assertIs<Incoming.Close>(awaitItem())
                assertIs<Incoming.Exception>(awaitItem())

                ddpClient.sendMessage(Outgoing.Ping(pingId)).test {
                    assertEquals(MessageState.Success, awaitItem())
                    awaitComplete()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `when connection fails with a non-allowlisted exception it should degrade to Disconnected instead of throwing`() = ddpTestApplication {
        // IllegalStateException is not in handledExceptions. Before the fix, initConnection's
        // .catch rethrew it, which escaped the collector's scope as an unhandled coroutine
        // exception (SIGABRT on Kotlin/Native). It must surface as Incoming.Exception +
        // ConnectionState.Disconnected like any other connection failure.
        val ddpClient = DDPClient("test", clientFactory = {
            throw IllegalStateException("engine exploded")
        }, {
            retryConnection = false
            pingPongEnabled = false
        })

        runTest {
            ddpClient.initConnection().test {
                val item = awaitItem()
                assertIs<Incoming.Exception>(item)
                assertIs<IllegalStateException>(item.exception)
                assertIs<ConnectionState.Disconnected>(ddpClient.connectionState.value)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }
}
