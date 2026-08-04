package io.bordo.ddpclient

import app.cash.turbine.test
import co.touchlab.kermit.Logger
import io.bordo.ddpclient.ddpclient.AuthenticationState
import io.bordo.ddpclient.ddpclient.Incoming
import io.bordo.ddpclient.ddpclient.MethodState
import io.bordo.ddpclient.ddpclient.Outgoing
import io.bordo.ddpclient.ddpclient.ResponseError
import io.bordo.ddpclient.ddpclient.TimeoutException
import io.bordo.ddpclient.ddpclient.defaultJson
import io.bordo.ddpclient.utils.IgnoreNative
import io.bordo.ddpclient.utils.TestDDPClient
import io.bordo.ddpclient.utils.receive
import io.bordo.ddpclient.utils.testAfterConnected
import io.bordo.ddpclient.utils.testServer
import io.bordo.ddpclient.utils.ddpTestApplication
import io.ktor.server.websocket.sendSerialized
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Created by Osman Saral on 10.04.2023
 */
class MethodTests {

    @Serializable
    data class User(val id: String, val name: String)

    private val json = defaultJson

    @Test
    @IgnoreNative
    fun `when ddpClient calls a method it should receive loading first`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        testServer {
            receive { }
        }

        ddpClient.initConnection().testAfterConnected(Duration.INFINITE) {
            ddpClient.call<User>("users").test(Duration.INFINITE) {
                val state = awaitItem()

                assertEquals(MethodState.Loading, state)

                ddpClient.closeConnection()
                cancelAndIgnoreRemainingEvents()
            }
            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when ddpClient calls a method with params server should be able to receive them`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedParams = buildJsonArray {
            add(
                buildJsonObject { put("foo", JsonPrimitive("bar")) },
            )
        }

        testServer {
            receive {
                assertIs<Outgoing.Method>(it)
                assertEquals(expectedParams, it.params)
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.call<User>("users", expectedParams)

            ddpClient.closeConnection()
            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when server responds with error ddpClient should be able to receive it`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedError = ResponseError("404", "reason", "message", "type")

        testServer {
            receive {
                assertIs<Outgoing.Method>(it)
                val response = Incoming.Result(it.id, expectedError, null)
                sendSerialized(response)
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.call<Unit>("method").test {
                assertEquals(MethodState.Loading, awaitItem())

                val state = awaitItem()
                assertIs<MethodState.Error>(state)
                assertEquals(expectedError, state.error)

                ddpClient.closeConnection()
                cancelAndIgnoreRemainingEvents()
            }

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when server responds with unauthorized error ddpClient should retry the call`() = ddpTestApplication {
        val ddpClient = TestDDPClient {
            this.unauthorizedChecker = { name, _, error ->
                error.error == "unauthorized"
            }
            this.tokenRefresher = {
                   delay(200)
                   true
            }
        }
        // refreshToken() is a no-op unless the client already considers itself Authorized, so
        // without this the tokenRefresher never runs and the retry path is never exercised.
        ddpClient.setAuthenticationState(AuthenticationState.Authorized)
        val responseError = ResponseError("unauthorized", "reason", "message", "type")
        val expectedUser = User("foo", "bar")

        testServer {
            receive {
                assertIs<Outgoing.Method>(it)
                val errorResponse = Incoming.Result(it.id, responseError, null)
                sendSerialized(errorResponse)

                delay(200)
                val updated = Incoming.Updated(listOf(it.id))
                sendSerialized(updated)

                delay(200)
                val successResponse = Incoming.Result(it.id, null, json.encodeToJsonElement(expectedUser).jsonObject)
                sendSerialized(successResponse)
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.call<User>("method").test {
                assertEquals(MethodState.Loading, awaitItem())

                assertIs<MethodState.Updated>(awaitItem())
                val state = awaitItem()
                assertIs<MethodState.Success<User>>(state)
                assertEquals(expectedUser, state.response)
                ddpClient.closeConnection()
                cancelAndIgnoreRemainingEvents()
            }

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when server responds with unauthorized error ddpClient should try to login only once`() = ddpTestApplication {
        var tokenRefresherCallCount = 0
        val ddpClient = TestDDPClient {
            this.unauthorizedChecker = { name, _, error ->
                error.error == "unauthorized"
            }
            this.tokenRefresher = {
                tokenRefresherCallCount++
                println("token refresher $tokenRefresherCallCount authorizing")
                delay(200)
                println("token refresher $tokenRefresherCallCount authorized")
                true
            }
        }
        ddpClient.setAuthenticationState(AuthenticationState.Authorized)
        val responseError = ResponseError("unauthorized", "reason", "message", "type")
        val expectedUser = User("foo", "bar")

        testServer {
            // Read BOTH calls before answering either, then emit the two errors back-to-back.
            // Overlapping re-auth flows are the whole point of the "only once" guarantee, and any
            // server round-trip between the errors leaves room for the first refresh to finish
            // first -- at which point a second refresh is legitimate and the assertion below is
            // testing nothing. (The original sent error, delay(400), success per call, which made
            // two refreshes the *normal* outcome.)
            val pendingIds = mutableListOf<String>()
            receive(2) {
                assertIs<Outgoing.Method>(it)
                pendingIds += it.id
            }
            pendingIds.forEach { sendSerialized(Incoming.Result(it, responseError, null)) }

            // The two resends that follow the single token refresh.
            receive(2) {
                assertIs<Outgoing.Method>(it)
                sendSerialized(
                    Incoming.Result(it.id, null, json.encodeToJsonElement(expectedUser).jsonObject)
                )
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.call<User>("method1").test {
                assertEquals(MethodState.Loading, awaitItem())

                ddpClient.call<User>("method2").test {
                    assertEquals(MethodState.Loading, awaitItem())

                    // Both calls get an unauthorized error and both enter the re-auth flow, but
                    // refreshToken() only refreshes while the state is Authorized -- the first one
                    // flips it to Authorizing, so the second must not call the refresher again.
                    // Awaiting the retried Success is what proves the refresh actually completed;
                    // asserting the counter straight after Loading just races the server.
                    assertIs<MethodState.Success<User>>(awaitItem())
                    assertEquals(1, tokenRefresherCallCount)

                    ddpClient.closeConnection()
                    cancelAndIgnoreRemainingEvents()
                }
                cancelAndIgnoreRemainingEvents()
            }

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when unauthorized retry fails it should show the original error`() = ddpTestApplication {
        val ddpClient = TestDDPClient {
            this.unauthorizedChecker = { _, _, error ->
                error.error == "unauthorized"
            }
            this.tokenRefresher = {
                false //assumes login unsuccessful
            }
        }
        // Without this the test passed for the wrong reason: refreshToken() short-circuited, so
        // the assertion saw MethodState.Error without the tokenRefresher ever being consulted.
        ddpClient.setAuthenticationState(AuthenticationState.Authorized)
        val expectedError = ResponseError("unauthorized", "reason", "message", "type")

        testServer {
            receive {
                assertIs<Outgoing.Method>(it)
                val errorResponse = Incoming.Result(it.id, expectedError, null)
                sendSerialized(errorResponse)
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.call<Unit>("method").test {
                assertEquals(MethodState.Loading, awaitItem())

                val state = awaitItem()
                println("state $state")
                assertIs<MethodState.Error>(state)
                assertEquals(expectedError, state.error)
                ddpClient.closeConnection()
                cancelAndIgnoreRemainingEvents()
            }

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when server responds with result ddpClient should be able to receive it`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedUser = User("foo", "bar")

        testServer {
            receive {
                assertIs<Outgoing.Method>(it)
                val response =
                    Incoming.Result(it.id, null, json.encodeToJsonElement(expectedUser).jsonObject)
                sendSerialized(response)
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.call<User>("method").test {
                assertEquals(MethodState.Loading, awaitItem())

                val state = awaitItem()
                println("state $state")
                assertIs<MethodState.Success<User>>(state)
                assertEquals(expectedUser, state.response)

                ddpClient.closeConnection()
                cancelAndIgnoreRemainingEvents()
            }

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when server responds with empty result ddpClient should be able to receive success`() = ddpTestApplication {
        val ddpClient = TestDDPClient()

        testServer {
            receive {
                assertIs<Outgoing.Method>(it)
                val response =
                    Incoming.Result(it.id, null, null)
                sendSerialized(response)
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.call<Unit>("method").test {
                assertEquals(MethodState.Loading, awaitItem())

                val state = awaitItem()
                assertIs<MethodState.Success<Unit>>(state)
                assertNull(state.response)

                ddpClient.closeConnection()
                cancelAndIgnoreRemainingEvents()
            }

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when server responds with updated message method state should be updated`() = ddpTestApplication {
        val ddpClient = TestDDPClient()

        testServer {
            receive {
                assertIs<Outgoing.Method>(it)
                val updated = Incoming.Updated(listOf(it.id))
                sendSerialized(updated)
                val response = Incoming.Result(it.id, null, null)
                sendSerialized(response)
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.call<Unit>("method").test {
                assertEquals(MethodState.Loading, awaitItem())

                var state = awaitItem()
                assertEquals(MethodState.Updated, state)

                state = awaitItem()
                assertIs<MethodState.Success<Unit>>(state)
                assertNull(state.response)
                ddpClient.closeConnection()
                awaitComplete()
            }

            cancelAndConsumeRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when server doesn't respond but connection is alive it should timeout immediately`() = ddpTestApplication {
        val ddpClient = TestDDPClient {
            timeout = 1000
            pingPongTimeout = 300
        }

        var receiveCount = 0

        testServer {
            receive(10) { // Expect more messages to prevent premature closure
                when (receiveCount++) {
                    0 -> {
                        // First method call - don't respond to trigger timeout
                        assertIs<Outgoing.Method>(it)
                    }
                    1 -> {
                        // Ping from checkConnection - respond immediately (connection is alive)
                        assertIs<Outgoing.Ping>(it)
                        sendSerialized(Incoming.Pong(it.id))
                    }
                    // Keep connection open for any additional messages
                }
            }
        }

        ddpClient.initConnection().testAfterConnected(Duration.INFINITE) {
            ddpClient.call<Unit>("method").test(Duration.INFINITE) {
                assertEquals(MethodState.Loading, awaitItem())

                // Should receive timeout exception immediately after connection check passes
                val state = awaitItem()
                assertIs<MethodState.Exception>(state)
                assertIs<TimeoutException>(state.exception)

                ddpClient.closeConnection()
                cancelAndIgnoreRemainingEvents()
            }

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    @IgnoreNative
    fun `when server doesn't respond and connection fails it should retry after reconnection`() = ddpTestApplication {
        val ddpClient = TestDDPClient {
            timeout = 1000
            pingPongTimeout = 300
        }

        var receiveCount = 0
        var methodId: String? = null

        testServer {
            receive(10) { // Expect more messages to prevent premature closure
                when (receiveCount++) {
                    0 -> {
                        // First method call - save ID and don't respond to trigger timeout
                        assertIs<Outgoing.Method>(it)
                        methodId = it.id
                    }
                    1 -> {
                        // Ping from connection check - don't respond (connection check fails)
                        assertIs<Outgoing.Ping>(it)
                    }
                    2 -> {
                        // Second method call after reconnection - respond with success
                        assertIs<Outgoing.Method>(it)
                        assertEquals(methodId, it.id)

                        // Small delay before responding
                        delay(100)

                        sendSerialized(Incoming.Result(
                            id = it.id,
                            result = null,
                            error = null
                        ))
                    }
                }
            }
        }

        ddpClient.initConnection().testAfterConnected(Duration.INFINITE) {
            ddpClient.call<Unit>("method").test(Duration.INFINITE) {
                assertEquals(MethodState.Loading, awaitItem())

                // Should receive success after reconnection and retry
                val successState = awaitItem()
                assertIs<MethodState.Success<Unit>>(successState)

                ddpClient.closeConnection()
                cancelAndIgnoreRemainingEvents()
            }

            cancelAndIgnoreRemainingEvents()
        }
    }
}
