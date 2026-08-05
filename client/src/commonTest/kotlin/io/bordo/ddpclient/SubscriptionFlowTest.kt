package io.bordo.ddpclient

import app.cash.turbine.test
import io.bordo.ddpclient.ddpclient.ResponseError
import io.bordo.ddpclient.ddpclient.Subscription
import io.bordo.ddpclient.ddpclient.SubscriptionState
import io.bordo.ddpclient.ddpclient.UnsubscribeAllState
import io.bordo.ddpclient.ddpclient.asSubscriptionFlow
import io.bordo.ddpclient.ddpclient.combineStates
import io.bordo.ddpclient.ddpclient.onEachSubscription
import io.bordo.ddpclient.ddpclient.subscriptionFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertSame

class SubscriptionFlowTest {

    private val error = ResponseError("nosub", "reason", "message", "type")

    // ---- Subscription ----------------------------------------------------------------------

    @Test
    fun `a subscription gets a unique id and starts Subscribing`() {
        val a = Subscription(name = "users")
        val b = Subscription(name = "users")

        assertNotEquals(a.id, b.id)
        assertEquals(SubscriptionState.Subscribing, a.state)
    }

    @Test
    fun `subscriptionMessage carries id name and params`() {
        val params = buildJsonArray { add(JsonPrimitive("p")) }
        val subscription = Subscription(id = "s1", name = "users", params = params)

        val message = subscription.subscriptionMessage
        assertEquals("s1", message.id)
        assertEquals("users", message.name)
        assertEquals(params, message.params)
    }

    // ---- SubscriptionFlow ------------------------------------------------------------------

    @Test
    fun `asSubscriptionFlow exposes the subscription alongside the stream`() = runTest {
        val subscription = Subscription(id = "s1", name = "users")
        val flow = flowOf(subscription).asSubscriptionFlow(subscription)

        assertSame(subscription, flow.subscription)
        flow.test {
            assertEquals(subscription, awaitItem())
            awaitComplete()
        }
    }

    @Test
    fun `subscriptionFlow stamps each emitted state onto a copy of the subscription`() = runTest {
        val subscription = Subscription(id = "s1", name = "users")

        val flow = subscriptionFlow(subscription) {
            send(SubscriptionState.Subscribing)
            send(SubscriptionState.Subscribed)
            send(SubscriptionState.Error(error))
        }

        flow.test {
            assertEquals(subscription.copy(state = SubscriptionState.Subscribing), awaitItem())
            assertEquals(subscription.copy(state = SubscriptionState.Subscribed), awaitItem())
            assertEquals(subscription.copy(state = SubscriptionState.Error(error)), awaitItem())
            awaitComplete()
        }
    }

    @Test
    fun `subscriptionFlow onEach observes every emission`() = runTest {
        val seen = mutableListOf<SubscriptionState>()
        val subscription = Subscription(id = "s1", name = "users")

        subscriptionFlow(subscription, onEach = { seen += it.state }) {
            send(SubscriptionState.Subscribing)
            send(SubscriptionState.Subscribed)
        }.test {
            awaitItem(); awaitItem(); awaitComplete()
        }

        assertEquals(listOf(SubscriptionState.Subscribing, SubscriptionState.Subscribed), seen)
    }

    @Test
    fun `onEachSubscription runs the action for every emission`() = runTest {
        val subscription = Subscription(id = "s1", name = "users")
        val subscribed = subscription.copy(state = SubscriptionState.Subscribed)
        val seen = mutableListOf<SubscriptionState>()

        flowOf(subscription, subscribed)
            .asSubscriptionFlow(subscription)
            .onEachSubscription { seen += it.state }
            .test {
                awaitItem(); awaitItem(); awaitComplete()
            }

        // Regression: this was `also { onEach(action) }`, which discarded the decorated flow, so
        // `seen` stayed empty no matter how the result was collected.
        assertEquals(listOf(SubscriptionState.Subscribing, SubscriptionState.Subscribed), seen)
    }

    @Test
    fun `onEachSubscription keeps the subscription so it stays chainable`() = runTest {
        val subscription = Subscription(id = "s1", name = "users")
        val flow = flowOf(subscription).asSubscriptionFlow(subscription)

        // Identity cannot be preserved -- a working onEach must return a new flow -- but the
        // `subscription` property is the whole reason SubscriptionFlow exists, so it must survive.
        assertSame(subscription, flow.onEachSubscription { }.subscription)
    }

    // ---- combineStates ---------------------------------------------------------------------
    // Drives unsubscribeFromAll's progress reporting; every branch is asserted because the else
    // branch throws, so an unmapped combination is a crash rather than a wrong value.

    private suspend fun combined(vararg states: SubscriptionState): UnsubscribeAllState {
        var result: UnsubscribeAllState? = null
        states.map { MutableStateFlow(it) }.combineStates().test {
            result = awaitItem()
            cancelAndIgnoreRemainingEvents()
        }
        return result!!
    }

    @Test
    fun `all unsubscribed reports UnsubscribedAll`() = runTest {
        assertEquals(
            UnsubscribeAllState.UnsubscribedAll,
            combined(SubscriptionState.Unsubscribed, SubscriptionState.Unsubscribed),
        )
    }

    @Test
    fun `all unsubscribing or unsubscribed reports StartedAll`() = runTest {
        assertEquals(
            UnsubscribeAllState.StartedAll,
            combined(SubscriptionState.Unsubscribing, SubscriptionState.Unsubscribed),
        )
    }

    @Test
    fun `a mix of subscribed and unsubscribing reports StartedSome`() = runTest {
        assertEquals(
            UnsubscribeAllState.StartedSome,
            combined(SubscriptionState.Subscribed, SubscriptionState.Unsubscribing),
        )
    }

    @Test
    fun `nothing unsubscribing yet reports NotStarted`() = runTest {
        assertEquals(
            UnsubscribeAllState.NotStarted,
            combined(SubscriptionState.Subscribed, SubscriptionState.Subscribing),
        )
    }

    @Test
    fun `an error state has no mapping and fails the flow`() = runTest {
        // Documents a real sharp edge: unsubscribing while one subscription is in Error fails the
        // combined flow rather than degrading to a state.
        listOf(
            MutableStateFlow<SubscriptionState>(SubscriptionState.Subscribed),
            MutableStateFlow<SubscriptionState>(SubscriptionState.Error(error)),
        ).combineStates().test {
            assertIs<IllegalStateException>(awaitError())
        }
    }
}
