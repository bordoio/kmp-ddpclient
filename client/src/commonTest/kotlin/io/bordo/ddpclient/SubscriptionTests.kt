package io.bordo.ddpclient

import app.cash.turbine.ReceiveTurbine
import app.cash.turbine.test
import io.bordo.ddpclient.db.receiveCollection
import io.bordo.ddpclient.ddpclient.AuthenticationState
import io.bordo.ddpclient.ddpclient.Incoming
import io.bordo.ddpclient.ddpclient.Outgoing
import io.bordo.ddpclient.ddpclient.ResponseError
import io.bordo.ddpclient.ddpclient.SubscriptionState
import io.bordo.ddpclient.ddpclient.UnsubscribeAllState
import io.bordo.ddpclient.ddpclient.combineStates
import io.bordo.ddpclient.ddpclient.defaultJson
import io.bordo.ddpclient.utils.IgnoreNative
import io.bordo.ddpclient.utils.TestDDPClient
import io.bordo.ddpclient.utils.awaitItemEqualTo
import io.bordo.ddpclient.utils.receive
import io.bordo.ddpclient.utils.testAfterConnected
import io.bordo.ddpclient.utils.testServer
import io.bordo.ddpclient.utils.ddpTestApplication
import io.ktor.server.websocket.sendSerialized
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.seconds

/**
 * Created by Osman Saral on 3.04.2023
 */
@ExperimentalCoroutinesApi
class SubscriptionTests {

    @Serializable
    data class ServerUser(val name: String, val surname: String)

    @Serializable
    data class User(@SerialName("_id") val id: String, val name: String, val surname: String? = null)

    private val json = defaultJson

    @Test
    @IgnoreNative
    fun `when server sends a data message ddpClient should deserialize it correctly`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedServerUser = ServerUser("foo", "bar")

        testServer {
            val json = json.encodeToJsonElement(expectedServerUser).jsonObject
            val added = Incoming.Added("user", "123", json)
            sendSerialized(added)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                val added = awaitItem()
                assertIs<Incoming.Added>(added)
                val serverUser = json.decodeFromJsonElement<ServerUser>(added.fields)

                assertEquals(expectedServerUser, serverUser)

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server sends an added message ddpClient should receive data object`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedUser = User("123", "foo", "bar")
        val expectedId = "123"

        testServer {
            val json = json.encodeToJsonElement(ServerUser("foo", "bar")).jsonObject
            val added = Incoming.Added("user", expectedId, json)
            sendSerialized(added)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.db.receiveCollection<User>("user").test {
                    assertEquals(listOf(), awaitItem())
                    val users = awaitItem()

                    assertEquals(listOf(expectedUser), users)

                    ddpClient.closeConnection()
                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server sends an added messages ddpClient should receive all objects`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val repeat = 50
        testServer {
            val serverUser = json.encodeToJsonElement(ServerUser("foo", "bar")).jsonObject

            repeat(repeat) {
                sendSerialized(Incoming.Added("user", it.toString(), serverUser))
                delay(10)
            }

            val changedJson = json.encodeToJsonElement(ServerUser("foo", "baz")).jsonObject
            val changed = Incoming.Changed("user", "2", changedJson, null)
            sendSerialized(changed)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.db.receiveCollection<User>("user").test(10.seconds) {
                    // receiveCollection debounces, so whenever two `added` frames land inside the
                    // same window they collapse into one emission and the size jumps (14 -> 17).
                    // Asserting one emission per document therefore asserted the *absence* of
                    // debouncing -- and ReceiveCollectionTest asserts the opposite for a burst.
                    // What is guaranteed is only that the collection settles on the full set with
                    // the `changed` applied; intermediate emissions are not ordered or sized in any
                    // contracted way (an empty snapshot can still arrive after a non-empty one).
                    var users = awaitItem()
                    while (users.size < repeat || users.find { it.id == "2" }?.surname != "baz") {
                        users = awaitItem()
                    }

                    assertEquals(repeat, users.size)
                    assertEquals("baz", users.find { it.id == "2" }?.surname)

                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server sends more objects than the page window the caller keeps one settled page`() = ddpTestApplication {
        val ddpClient = TestDDPClient()

        testServer {
            val serverUser = json.encodeToJsonElement(ServerUser("foo", "bar")).jsonObject
            sendSerialized(Incoming.Added("user", "1", serverUser))
            sendSerialized(Incoming.Added("user", "2", serverUser))
            sendSerialized(Incoming.Added("user", "3", serverUser))
            sendSerialized(Incoming.Added("user", "4", serverUser))
            sendSerialized(Incoming.Added("user", "5", serverUser))
            delay(500)

            val changedJson = json.encodeToJsonElement(ServerUser("foo", "baz")).jsonObject
            val changed = Incoming.Changed("user", "2", changedJson, null)
            sendSerialized(changed)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.db.receiveCollection<User>(name = "user") { true }
                    // the window lives in the caller now, as it does in MessageFlowUseCase
                    .map { users -> users.take(3) }
                    .test {
                        // The burst settles into a full page of 3 out of the 5 sent, then the
                        // change to user 2 is reflected in place rather than duplicating it.
                        var users = awaitItem()
                        while (users.size != 3 || users.find { it.id == "2" }?.surname != "baz") {
                            users = awaitItem()
                        }
                        assertEquals(3, users.size)
                        assertEquals(1, users.count { it.id == "2" })

                        cancelAndIgnoreRemainingEvents()
                    }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server sends an all items are removed empty list should be emitted`() = ddpTestApplication {
        val ddpClient = TestDDPClient()

        testServer {
            val serverUser = json.encodeToJsonElement(ServerUser("foo", "bar")).jsonObject
            sendSerialized(Incoming.Added("user", "1", serverUser))
            delay(500)
            sendSerialized(Incoming.Removed("user", "1"))
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.db.receiveCollection<User>(name = "user") { true }.test {
                    // The empty snapshot the collection starts from is only emitted when it lasts
                    // longer than the burst settle, so tolerate it either way.
                    val users = awaitItem().ifEmpty { awaitItem() }
                    assertEquals(1, users.size, "correct size should be emitted")
                    assertEquals(0, awaitItem().size, "empty collection should be emitted after the removal")
                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server sends an added message ddpClient should receive data object paginated`() = ddpTestApplication {
        val ddpClient = TestDDPClient()

        val users = listOf(
            ServerUser("haci", "active"),
            ServerUser("oğuzhan", "active"),
            ServerUser("osman", "active"),
            ServerUser("elif", "waiting"),
            ServerUser("abdullah", "active"),
            ServerUser("yunuscan", "closed"),
            ServerUser("oğuz v", "active"),
            ServerUser("oğuz k", "closed"),
            ServerUser("furkan", "active"),
            ServerUser("ercan", "closed"),
        )

        testServer {
            users.forEachIndexed { index, serverUser ->
                val json = json.encodeToJsonElement(serverUser).jsonObject
                val added = Incoming.Added("user", (index + 1).toString(), json)
                sendSerialized(added)
            }
        }

        // the window a paging caller applies, as MessageFlowUseCase and SessionFlowRepository do
        fun pagedUsers(page: Int, pageSize: Int = 4, filter: (User) -> Boolean) =
            ddpClient.db.receiveCollection("user", filter).map { it.take(page * pageSize) }

        // The added burst settles into a full page; if delivery straddles the settle window an
        // earlier partial page can be emitted first, so wait for the page to fill. A broken
        // implementation never reaches [expected] and fails on turbine's timeout.
        suspend fun ReceiveTurbine<List<User>>.awaitPage(expected: Int): List<User> {
            while (true) {
                val users = awaitItem()
                if (users.size == expected) return users
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                pagedUsers(page = 1) { true }.test {
                    awaitPage(4)
                    cancelAndIgnoreRemainingEvents()
                }

                pagedUsers(page = 2) { true }.test {
                    awaitPage(8)
                    cancelAndIgnoreRemainingEvents()
                }

                pagedUsers(page = 1) { it.surname == "waiting" }.test {
                    val users = awaitPage(1)
                    for (user in users) {
                        assertEquals("waiting", user.surname)
                    }

                    cancelAndIgnoreRemainingEvents()
                }

                pagedUsers(page = 1) { it.surname == "active" }.test {
                    val users = awaitPage(4)
                    for (user in users) {
                        assertEquals("active", user.surname)
                    }

                    cancelAndIgnoreRemainingEvents()
                }

                pagedUsers(page = 2) { it.surname == "active" }.test {
                    val users = awaitPage(6)
                    for (user in users) {
                        assertEquals("active", user.surname)
                    }

                    cancelAndIgnoreRemainingEvents()
                }

                pagedUsers(page = 1) { it.surname == "closed" }.test {
                    val users = awaitPage(3)
                    for (user in users) {
                        assertEquals("closed", user.surname)
                    }

                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server doesn't send an added message ddpClient should receive empty list`() = ddpTestApplication {
            val ddpClient = TestDDPClient()

            val users = listOf<ServerUser>()

            testServer {
                users.forEachIndexed { index, serverUser ->
                    val json = json.encodeToJsonElement(serverUser).jsonObject
                    val added = Incoming.Added("user", (index + 1).toString(), json)
                    sendSerialized(added)
                }
            }

            runTest {
                ddpClient.initConnection().testAfterConnected {
                    ddpClient.db.receiveCollection<User>("user") { true }
                        .test {
                            assertEquals(0, awaitItem().size)

                            cancelAndIgnoreRemainingEvents()
                        }

                    cancelAndIgnoreRemainingEvents()
                }
            }
        }

    @Test
    @IgnoreNative
    fun `when server sends a change message ddpClient should receive the changed object`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedUser = User("123", "Bobby", "bar")
        val expectedId = "123"

        // assume it was added before
        val addedJson = json.encodeToJsonElement(ServerUser("Bob", "bar")).jsonObject
        ddpClient.db.onDataAdded("user", expectedId, addedJson)

        testServer {
            val changedJson = json.encodeToJsonElement(ServerUser("Bobby", "bar")).jsonObject
            val changed = Incoming.Changed("user", expectedId, changedJson, null)
            sendSerialized(changed)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.db.receiveCollection<User>("user")
                    .test {
                        assertEquals(listOf(), awaitItem())
                        awaitItem()
                        val users = awaitItem()

                        assertEquals(listOf(expectedUser), users)

                        ddpClient.closeConnection()
                        cancelAndIgnoreRemainingEvents()
                    }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server sends a change message with clear ddpClient should clear the field`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedUser = User("123", "Alice", null)
        val expectedId = "123"

        // assume it was added before
        val addedJson = json.encodeToJsonElement(ServerUser("Alice", "bar")).jsonObject
        ddpClient.db.onDataAdded("user", expectedId, addedJson)

        testServer {
            val changed = Incoming.Changed("user", expectedId, null, listOf("surname"))
            sendSerialized(changed)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.db.receiveCollection<User>("user").test {
                    awaitItemEqualTo(listOf(expectedUser))

                    ddpClient.closeConnection()
                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server sends a remove message with ddpClient should remove the data`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedId = "123"

        // assume it was added before
        val addedJson = json.encodeToJsonElement(ServerUser("Alice", "bar")).jsonObject
        ddpClient.db.onDataAdded("user", expectedId, addedJson)

        testServer {
            val changed = Incoming.Removed("user", expectedId)
            sendSerialized(changed)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected(name = "connection") {
                ddpClient.db.receiveCollection<User>("user").test(name = "receive") {
                    awaitItemEqualTo(emptyList())

                    ddpClient.closeConnection()
                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server sends a remove message with non existing id ddpClient should ignore it`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        // assume it was added before
        val addedJson = json.encodeToJsonElement(ServerUser("Alice", "bar")).jsonObject
        ddpClient.db.onDataAdded("user", "123", addedJson)

        println("sizee:" + ddpClient.db.collections["user"]?.documents?.size)

        testServer {
            val changed = Incoming.Removed("user", "1234")
            sendSerialized(changed)
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.db.receiveCollection<User>("user").test {
                    val users = expectMostRecentItem()

                    assertEquals(1, users.size)

                    ddpClient.closeConnection()
                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when ddpClient sends sub message it should be able to receive ready message`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        testServer {
            receive {
                assertIs<Outgoing.Subscribe>(it)
                assertEquals("user", it.name)
                val id = it.id

                assertNotNull(id)

                sendSerialized(Incoming.Ready(listOf(id)))
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.subscribe("user").test {
                    assertIs<SubscriptionState.Subscribing>(awaitItem().state)
                    assertIs<SubscriptionState.Subscribed>(awaitItem().state)

                    assertEquals(1, ddpClient.subscriptionCount)
                    ddpClient.closeConnection()
                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when ddpClient sends sub message it should be able to receive nosub message`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedError = ResponseError("404", "reason", "message", "type")
        testServer {
            receive {
                assertIs<Outgoing.Subscribe>(it)
                assertEquals("user", it.name)
                val id = it.id

                assertNotNull(id)

                sendSerialized(Incoming.NoSub(id, expectedError))
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.subscribe("user").test {
                    assertIs<SubscriptionState.Subscribing>(awaitItem().state)
                    val incoming = awaitItem().state
                    assertIs<SubscriptionState.Error>(incoming)
                    assertEquals(expectedError, incoming.error)

                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when subscription is called with params server should be able to receive them`() = ddpTestApplication {
        val ddpClient = TestDDPClient()
        val expectedParams = buildJsonArray {
            add(
                buildJsonObject { put("foo", JsonPrimitive("bar")) },
            )
        }

        testServer {
            receive {
                println("received message $it")
                assertIs<Outgoing.Subscribe>(it)
                assertEquals(expectedParams, it.params)
                println("asserts success")

                sendSerialized(Incoming.Ready(listOf(it.id)))
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.subscribe("user", expectedParams).test {
                    assertIs<SubscriptionState.Subscribing>(awaitItem().state)
                    assertIs<SubscriptionState.Subscribed>(awaitItem().state)
                    cancelAndIgnoreRemainingEvents()
                }
                assertIs<Incoming.Ready>(awaitItem())

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server sends multiple subscription ready messages client should handle them all`() = ddpTestApplication {
        val ddpClient = TestDDPClient()

        val subscriptionCount = 50

        val subscriptions = mutableListOf<String>()
        testServer {
            receive(subscriptionCount) {
                if (subscriptions.size < subscriptionCount) { //assert subscribes first
                    println("xxx1 receive $it")
                    assertIs<Outgoing.Subscribe>(it, "assert 1")
                    subscriptions.add(it.id)
                    println("xxx2 receive $it")
                }

                if (subscriptions.size == subscriptionCount) {
                    println("xxx5 receive $it")
                    sendSerialized(Incoming.Ready(subscriptions))
                }
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                repeat(subscriptionCount) {
                    launch {
                        ddpClient.subscribe("collection_$it").test {
                            assertIs<SubscriptionState.Subscribing>(awaitItem().state)
                            assertIs<SubscriptionState.Subscribed>(awaitItem().state)
                            cancelAndIgnoreRemainingEvents()
                        }
                    }
                }

                val ready = awaitItem().also { println("subs: $it") }
                assertIs<Incoming.Ready>(ready)
                assertEquals(subscriptions, ready.subs)
                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when unsubscribeFromAll called it should succeed`() = ddpTestApplication {
        val ddpClient = TestDDPClient()

        val subscriptionCount = 2
        var readySent = false

        val subscriptions = mutableListOf<String>()
        testServer {
            receive(subscriptionCount * 2) {
                println("xxx receive $it")
                if (subscriptions.size < subscriptionCount) { //assert subscribes first
                    println("xxx1 receive $it")
                    assertIs<Outgoing.Subscribe>(it, "assert 1")
                    subscriptions.add(it.id)
                    println("xxx2 receive $it")
                } else {
                    println("xxx3 receive $it")
                    assertIs<Outgoing.Unsubscribe>(it, "assert 2 size ${subscriptions.size}")
                    sendSerialized(Incoming.NoSub(it.id, null))
                    println("xxx4 receive $it")
                }
                if (subscriptions.size == subscriptionCount && !readySent) {
                    println("xxx5 receive $it")
                    sendSerialized(Incoming.Ready(subscriptions))
                    readySent = true
                }
            }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                repeat(subscriptionCount) {
                    launch {
                        ddpClient.subscribe("collection_$it").test {
                            assertIs<SubscriptionState.Subscribing>(awaitItem().state)
                            assertIs<SubscriptionState.Subscribed>(awaitItem().state)
                            cancelAndIgnoreRemainingEvents()
                        }
                    }
                }

                assertIs<Incoming.Ready>(awaitItem())
                assertEquals(subscriptionCount, ddpClient.subscriptionCount, "subscriptionCount should be $subscriptionCount")

                ddpClient.unsubscribeFromAll().test {

                    assertEquals(UnsubscribeAllState.StartedAll, awaitItem().also { println(it) })
                    assertEquals(UnsubscribeAllState.UnsubscribedAll, awaitItem().also { println(it) })

                    assertEquals(0, ddpClient.subscriptionCount, "subscriptionCount should be 0")
                    cancelAndIgnoreRemainingEvents()
                }

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `combine test`() = runTest {
        listOf(
            listOf(
                UnsubscribeAllState.NotStarted,
                UnsubscribeAllState.NotStarted
            ) to listOf(
                channelFlow<SubscriptionState> {
                    send(SubscriptionState.Subscribing)
                    delay(50)
                    send(SubscriptionState.Subscribed)
                },
                flowOf(SubscriptionState.Subscribed),
                flowOf(SubscriptionState.Subscribed),
                flowOf(SubscriptionState.Subscribed),
            ),
            listOf(
                UnsubscribeAllState.NotStarted,
                UnsubscribeAllState.StartedSome
            ) to listOf(
                channelFlow<SubscriptionState> {
                    send(SubscriptionState.Subscribed)
                    delay(50)
                    send(SubscriptionState.Unsubscribing)
                },
                flowOf(SubscriptionState.Subscribed),
                flowOf(SubscriptionState.Subscribed),
                flowOf(SubscriptionState.Subscribed),
            ),
            listOf(
                UnsubscribeAllState.NotStarted,
                UnsubscribeAllState.StartedSome,
                UnsubscribeAllState.StartedSome,
                UnsubscribeAllState.StartedSome,
                UnsubscribeAllState.StartedAll,
                UnsubscribeAllState.StartedAll,
                UnsubscribeAllState.StartedAll,
                UnsubscribeAllState.StartedAll,
                UnsubscribeAllState.UnsubscribedAll,
            ) to listOf(
                channelFlow {
                    send(SubscriptionState.Subscribed)
                    delay(50)
                    send(SubscriptionState.Unsubscribing)
                    delay(50)
                    send(SubscriptionState.Unsubscribed)
                },
                channelFlow {
                    send(SubscriptionState.Subscribed)
                    delay(50)
                    send(SubscriptionState.Unsubscribing)
                    delay(50)
                    send(SubscriptionState.Unsubscribed)
                },
                channelFlow {
                    send(SubscriptionState.Subscribed)
                    delay(50)
                    send(SubscriptionState.Unsubscribing)
                    delay(50)
                    send(SubscriptionState.Unsubscribed)
                },
                channelFlow {
                    send(SubscriptionState.Subscribed)
                    delay(50)
                    send(SubscriptionState.Unsubscribing)
                    delay(50)
                    send(SubscriptionState.Unsubscribed)
                },
            ),
        ).forEachIndexed { i, list ->
            println("item $i")
            list.second.combineStates().test {
                list.first.forEach {
                    assertEquals(it, awaitItem(),"error $i")
                }
                awaitComplete()
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when unsubscribeFromAll called it should succeed if there is no subscription`() = ddpTestApplication {
        val ddpClient = TestDDPClient()

        testServer {
            receive {  }
        }

        runTest {
            ddpClient.initConnection().testAfterConnected {
                ddpClient.unsubscribeFromAll().test {
                    assertEquals(UnsubscribeAllState.UnsubscribedAll, awaitItem())

                    assertEquals(0, ddpClient.subscriptionCount, "subscriptionCount should be 0")

                    awaitComplete()
                }
            }
        }
    }

    @Test
    @IgnoreNative
    fun `when server responds with unauthorized error ddpClient should retry the call`() = ddpTestApplication {
        val ddpClient = TestDDPClient {
            this.unauthorizedChecker = { _, _, error ->
                error.error == "unauthorized"
            }
            this.tokenRefresher = {
                true
            }
        }
        ddpClient.setAuthenticationState(AuthenticationState.Authorized) //DDP Client should think it's authorized
        val expectedError = ResponseError("unauthorized", "reason", "message", "type")

        // Keyed off the subscription id rather than a message counter, so a duplicate `sub` frame
        // is answered correctly instead of consuming the budget the real retry needs.
        val rejectedIds = mutableSetOf<String>()
        testServer {
            receive(4) {
                assertIs<Outgoing.Subscribe>(it)
                if (rejectedIds.add(it.id)) {
                    sendSerialized(Incoming.NoSub(it.id, expectedError))
                } else {
                    sendSerialized(Incoming.Ready(listOf(it.id)))
                }
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.subscribe("user").test {
                // Assert the outcome, not an exact emission count, for the same reason.
                var state = awaitItem().state
                while (state !is SubscriptionState.Subscribed) {
                    assertIs<SubscriptionState.Subscribing>(state)
                    state = awaitItem().state
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
        val expectedError = ResponseError("unauthorized", "reason", "message", "type")

        testServer {
            receive {
                assertIs<Outgoing.Subscribe>(it)
                val errorResponse = Incoming.NoSub(it.id, expectedError)
                sendSerialized(errorResponse)
            }
        }

        ddpClient.initConnection().testAfterConnected {
            ddpClient.subscribe("user").test {
                assertIs<SubscriptionState.Subscribing>(awaitItem().state)

                val state = awaitItem().state
                println("state $state")
                assertIs<SubscriptionState.Error>(state)
                assertEquals(expectedError, state.error)

                awaitComplete()
            }

            cancelAndIgnoreRemainingEvents()
        }
    }
}
