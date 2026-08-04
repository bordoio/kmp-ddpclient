package io.bordo.ddpclient.ddpclient

import co.touchlab.kermit.Logger
import io.bordo.ddpclient.util.randomUUID
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.serialization.json.JsonArray

/**
 * Created by Osman Saral on 6.04.2023
 */
interface SubscriptionFlow : Flow<Subscription> {
    val subscription: Subscription
}

fun Flow<Subscription>.asSubscriptionFlow(
    subscription: Subscription
): SubscriptionFlow {
    return SubscriptionFlowImpl(subscription, this)
}

//@OptIn(ExperimentalTypeInference::class)
//private fun subscriptionFlow(
//    name: String,
//    params: JsonArray? = null,
//    onEach: (Subscription) -> Unit = {},
//    block: suspend ProducerScope<SubscriptionState>.(Subscription) -> Unit
//): SubscriptionFlow = subscriptionFlow(
//    subscription = Subscription(name = name, params = params),
//    onEach = onEach,
//    block = block,
//)

fun subscriptionFlow(
    subscription: Subscription,
    onEach: (Subscription) -> Unit = {},
    block: suspend ProducerScope<SubscriptionState>.(Subscription) -> Unit
): SubscriptionFlow {
    return channelFlow { block(subscription) }.map {
        subscription.copy(state = it)
    }.onEach(onEach).asSubscriptionFlow(subscription)
}


fun SubscriptionFlow.onEachSubscription(action: suspend (Subscription) -> Unit) = also { onEach(action) }

fun SubscriptionFlow.launch(scope: CoroutineScope) = also { launchIn(scope) }

private class SubscriptionFlowImpl(
    override val subscription: Subscription,
    flow: Flow<Subscription>,
) : SubscriptionFlow, Flow<Subscription> by flow

sealed interface SubscriptionState {
    data object Subscribing: SubscriptionState
    data object Subscribed: SubscriptionState
    data object Unsubscribing: SubscriptionState
    data object Unsubscribed: SubscriptionState
    data class Error(val error: ResponseError?): SubscriptionState
}

data class Subscription(
    val id: String = randomUUID(),
    val name: String,
    val params: JsonArray? = null,
    val state: SubscriptionState = SubscriptionState.Subscribing
) {

    val subscriptionMessage
        get() = Outgoing.Subscribe(id, name, params)
}

fun List<Flow<SubscriptionState>>.combineStates() = combine(this) { states ->
    when {
        states.all { it == SubscriptionState.Unsubscribed } -> UnsubscribeAllState.UnsubscribedAll
        states.all { it == SubscriptionState.Unsubscribing || it == SubscriptionState.Unsubscribed } -> UnsubscribeAllState.StartedAll
        states.any { it == SubscriptionState.Unsubscribing || it == SubscriptionState.Unsubscribed } -> UnsubscribeAllState.StartedSome
        states.all { it is SubscriptionState.Subscribing || it is SubscriptionState.Subscribed } -> UnsubscribeAllState.NotStarted
        else -> throw IllegalStateException("Unexpected state combination: ${states.joinToString { it.toString() }}")
    }
}

sealed class UnsubscribeAllState {
    data object NotStarted: UnsubscribeAllState()
    data object StartedSome: UnsubscribeAllState()
    data object StartedAll: UnsubscribeAllState()
    data object UnsubscribedAll: UnsubscribeAllState()
}
