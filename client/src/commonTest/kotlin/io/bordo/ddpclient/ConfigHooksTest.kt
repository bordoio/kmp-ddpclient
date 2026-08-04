package io.bordo.ddpclient

import app.cash.turbine.test
import io.bordo.ddpclient.ddpclient.AuthenticationState
import io.bordo.ddpclient.ddpclient.ConnectionState
import io.bordo.ddpclient.ddpclient.Incoming
import io.bordo.ddpclient.ddpclient.MethodState
import io.bordo.ddpclient.ddpclient.Outgoing
import io.bordo.ddpclient.ddpclient.ResponseError
import io.bordo.ddpclient.ddpclient.defaultJson
import io.bordo.ddpclient.utils.IgnoreNative
import io.bordo.ddpclient.utils.TestDDPClient
import io.bordo.ddpclient.utils.ddpTestApplication
import io.bordo.ddpclient.utils.receive
import io.bordo.ddpclient.utils.testAfterConnected
import io.bordo.ddpclient.utils.testServer
import io.ktor.server.websocket.sendSerialized
import io.ktor.websocket.CloseReason
import io.ktor.websocket.close
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The `DDPClientConfig` lambdas are the library's extension points -- they are the reason it holds
 * no app-specific code -- and nothing exercised them directly.
 */
@ExperimentalCoroutinesApi
class ConfigHooksTest {

    private val unauthorized = ResponseError("unauthorized", "reason", "message", "type")

    @Test
    @IgnoreNative
    fun `unauthorizedChecker receives the method name params and error`() = ddpTestApplication {
        val params = buildJsonArray { add(JsonPrimitive("p1")) }
        var seenMethod: String? = null
        var seenParams: kotlinx.serialization.json.JsonArray? = null
        var seenError: ResponseError? = null

        val ddpClient = TestDDPClient {
            unauthorizedChecker = { method, callParams, error ->
                seenMethod = method
                seenParams = callParams
                seenError = error
                false // don't retry; we only care about what the hook was handed
            }
        }
        ddpClient.setAuthenticationState(AuthenticationState.Authorized)

        testServer {
            receive {
                assertIs<Outgoing.Method>(it)
                sendSerialized(Incoming.Result(it.id, unauthorized, null))
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.call<Unit>("users.update", params).test {
                    assertEquals(MethodState.Loading, awaitItem())
                    assertIs<MethodState.Error>(awaitItem())
                    ddpClient.closeConnection()
                    cancelAndIgnoreRemainingEvents()
                }
                cancelAndIgnoreRemainingEvents()
            }
        }

        assertEquals("users.update", seenMethod)
        assertEquals(params, seenParams)
        assertEquals(unauthorized, seenError)
    }

    @Test
    @IgnoreNative
    fun `unauthorizedChecker returning false surfaces the error without resending`() = ddpTestApplication {
        // Asserted on method sends rather than on tokenRefresher: the client also refreshes on
        // every successful Incoming.Connected, so a "refresher never ran" assertion would be
        // measuring the connect-time refresh, not the unauthorized path.
        var methodSends = 0
        val ddpClient = TestDDPClient {
            unauthorizedChecker = { _, _, _ -> false }
        }
        ddpClient.setAuthenticationState(AuthenticationState.Authorized)

        testServer {
            receive(2) {
                assertIs<Outgoing.Method>(it)
                methodSends++
                sendSerialized(Incoming.Result(it.id, unauthorized, null))
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.call<Unit>("method").test {
                    assertEquals(MethodState.Loading, awaitItem())
                    val state = awaitItem()
                    assertIs<MethodState.Error>(state)
                    // The error reaches the caller untouched -- the default config must not turn
                    // ordinary method errors into re-authentication attempts.
                    assertEquals(unauthorized, state.error)
                    ddpClient.closeConnection()
                    cancelAndIgnoreRemainingEvents()
                }
                cancelAndIgnoreRemainingEvents()
            }
        }

        assertEquals(1, methodSends, "the method was resent despite unauthorizedChecker saying no")
    }

    @Test
    @IgnoreNative
    fun `retryConnection false means a dropped connection is not retried`() = ddpTestApplication {
        var connects = 0
        val ddpClient = TestDDPClient { retryConnection = false }

        testServer {
            connects++
            close(CloseReason(CloseReason.Codes.NORMAL, "closed"))
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                assertIs<Incoming.Close>(awaitItem())
                assertIs<Incoming.Exception>(awaitItem())
                assertIs<ConnectionState.Disconnected>(ddpClient.connectionState.value)
                expectNoEvents()
            }
        }

        assertEquals(1, connects)
    }

    @Test
    fun `maxReconnectAttempts caps the retries within a single outage`() = runTest {
        // No server: the budget resets on every successful Incoming.Connected, so a server that
        // completes the handshake and then closes reconnects forever and never exhausts it. The
        // connection has to fail before Connected, which a throwing clientFactory does -- and that
        // also lets this run on native.
        //
        // ClosedReceiveChannelException specifically: retryWhen only retries allowlisted causes,
        // so an arbitrary exception would be a no-retry test by accident.
        var attempts = 0
        val ddpClient = io.bordo.ddpclient.ddpclient.DDPClient("test", clientFactory = {
            attempts++
            throw kotlinx.coroutines.channels.ClosedReceiveChannelException("no route")
        }, {
            retryConnection = true
            maxReconnectAttempts = 2
            retryDelay = 10
            pingPongEnabled = false
        })

        ddpClient.initConnection().test(timeout = 10.seconds) {
            assertIs<Incoming.Exception>(awaitItem())
            assertIs<ConnectionState.Disconnected>(ddpClient.connectionState.value)
            cancelAndIgnoreRemainingEvents()
        }

        assertEquals(3, attempts, "expected 1 initial attempt + maxReconnectAttempts retries")
    }

    @Test
    @IgnoreNative
    fun `pingPongEnabled false means the client never sends a keepalive ping`() = ddpTestApplication {
        val received = mutableListOf<Outgoing>()
        val ddpClient = TestDDPClient {
            pingPongEnabled = false
            pingPongInterval = 50
        }

        testServer {
            receive(1) { received += it }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                // Give the keepalive several intervals' worth of opportunity to fire.
                ddpClient.call<Unit>("noop")
                expectNoEvents()
                ddpClient.closeConnection()
                cancelAndIgnoreRemainingEvents()
            }
        }

        assertNull(received.filterIsInstance<Outgoing.Ping>().firstOrNull())
    }

    @Test
    @IgnoreNative
    fun `a custom json config is used for decoding method results`() = ddpTestApplication {
        // httpClientConfig/json are config surface a consumer is likely to override; this pins
        // that the configured Json instance -- not a private default -- does the decoding.
        val ddpClient = TestDDPClient { json = defaultJson }

        testServer {
            receive {
                assertIs<Outgoing.Method>(it)
                // An unknown key must not blow up: defaultJson sets ignoreUnknownKeys.
                sendSerialized(
                    Incoming.Result(
                        it.id,
                        null,
                        defaultJson.parseToJsonElement("""{"id":"1","name":"a","extra":true}"""),
                    )
                )
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.call<MethodTests.User>("method").test {
                    assertEquals(MethodState.Loading, awaitItem())
                    val state = awaitItem()
                    assertIs<MethodState.Success<MethodTests.User>>(state)
                    assertEquals(MethodTests.User("1", "a"), state.response)
                    ddpClient.closeConnection()
                    cancelAndIgnoreRemainingEvents()
                }
                cancelAndIgnoreRemainingEvents()
            }
        }
    }
}
