package io.bordo.ddpclient.ddpclient

import co.touchlab.kermit.Logger
import co.touchlab.stately.collections.ConcurrentMutableMap
import io.bordo.ddpclient.db.Database
import io.bordo.ddpclient.db.memory.InMemoryDatabase
import io.bordo.ddpclient.util.platformExceptions
import io.bordo.ddpclient.util.randomUUID
import io.ktor.client.HttpClient
import io.ktor.client.HttpClientConfig
import io.ktor.client.network.sockets.ConnectTimeoutException
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.WebSocketException
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.sendSerialized
import io.ktor.client.plugins.websocket.webSocketSession
import io.ktor.http.URLProtocol
import io.ktor.http.set
import io.ktor.util.network.UnresolvedAddressException
import io.ktor.websocket.CloseReason
import io.ktor.websocket.close
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.transformWhile
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.decodeFromJsonElement
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract
import kotlin.coroutines.cancellation.CancellationException
import kotlin.reflect.KClass
import kotlinx.coroutines.CancellationException as KotlinxCancellationException

/**
 * Created by Osman Saral on 20.03.2023
 */

val defaultJson: Json
    get() = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = true
        isLenient = true
        coerceInputValues = true
    }

private val defaultExceptions: Set<KClass<out Exception>>
    get() = setOf(
        ClosedReceiveChannelException::class,
        ClosedSendChannelException::class,
        UnresolvedAddressException::class,
        ConnectTimeoutException::class,
        WebSocketException::class,
        CancellationException::class,
        KotlinxCancellationException::class
    )

private val handledExceptions = defaultExceptions + platformExceptions

@OptIn(ExperimentalContracts::class)
private fun Throwable.isHandled(): Boolean {
    contract {
        returns(true) implies (this@isHandled is Exception)
    }
    return this is Exception && handledExceptions.any { it.isInstance(this) }
}

private fun HttpClientConfig<*>.defaultConfig(
    json: Json,
) {
    install(WebSockets) {
        contentConverter = DDPMessageConverter(json)
    }
}

class DDPClientConfig {
    var json: Json = defaultJson
    var protocol = URLProtocol.WSS
    var socketPath: String? = Utils.generateSocketPath()
    var db: Database = InMemoryDatabase(json)
    var httpClientConfig: HttpClientConfig<*>.() -> Unit = {}
    var maxReconnectAttempts = DEFAULT_RECONNECT_ATTEMPTS_MAX
    var ddpVersion = DDPClient.SUPPORTED_DDP_VERSIONS[0]
    var retryDelay = DEFAULT_RETRY_DELAY
    var retryConnection = true
    var timeout: Long = DEFAULT_TIMEOUT
    var pingPongInterval: Long = DEFAULT_PING_PONG_INTERVAL
    var pingPongTimeout: Long = DEFAULT_PING_PONG_TIMEOUT
    var pingPongEnabled: Boolean = true
    var tokenRefresher: suspend () -> Boolean = { true }
    var unauthorizedChecker: (String, JsonArray?, ResponseError) -> Boolean = { _, _, _ -> false }

    fun getClient(httpClient: HttpClient): HttpClient {
        return httpClient.config {
            defaultConfig(json)
            httpClientConfig()
        }
    }

    companion object {
        private const val DEFAULT_RECONNECT_ATTEMPTS_MAX = 3
        private const val DEFAULT_RETRY_DELAY = 500L
        // 20s (not 10s): heavy server ops like business.tenants.create legitimately take >10s, and a
        // shorter deadline surfaced a false "request timeout" even though the server had succeeded.
        private const val DEFAULT_TIMEOUT = 20_000L
        private const val DEFAULT_PING_PONG_INTERVAL = 30_000L
        private const val DEFAULT_PING_PONG_TIMEOUT = 15_000L
    }
}

typealias ClientFactory = ((HttpClientConfig<*>) -> Unit) -> HttpClient

class DDPClient(
    var host: String,
    block: DDPClientConfig.() -> Unit = {},
) {
    private val config = DDPClientConfig().apply(block)

    var clientFactory: ClientFactory

    constructor(
        host: String,
        clientFactory: ClientFactory,
        block: DDPClientConfig.() -> Unit = {},
    ) : this(host, block) {
        this.clientFactory = clientFactory
    }

    init {
        clientFactory = {
            HttpClient {
                config.httpClientConfig(this)
                it(this)
            }
        }
    }

    val json = config.json
    val db = config.db
    val socketPath = config.socketPath
    var ddpVersion = config.ddpVersion
    private val protocol = config.protocol
    private var maxReconnectAttempts = config.maxReconnectAttempts
    private var retryDelay = config.retryDelay
    private var retryConnection = config.retryConnection
    var tokenRefresher = config.tokenRefresher
    var unauthorizedChecker = config.unauthorizedChecker
    var timeout = config.timeout
    var pingPongInterval = config.pingPongInterval
    var pingPongTimeout = config.pingPongTimeout
    var pingPongEnabled = config.pingPongEnabled

    // Consecutive failed reconnects since the last successful Incoming.Connected. retryWhen's own
    // `attempt` is monotonic for the whole flow lifetime, so it treated maxReconnectAttempts as a
    // *lifetime* budget: after the socket had dropped and recovered that many times over a session
    // (TLS blips at launch, backgrounding, the server closing the socket with code NORMAL after
    // signup/verify), the next drop exhausted it and the client was stranded Disconnected forever.
    // Reset to 0 on every successful connect (see the receive loop) so the budget is per outage.
    private var connectionRetryCount = 0
    
    val httpClient: HttpClient by lazy { config.getClient(clientFactory(config.httpClientConfig)) }

    var webSocketSession: DefaultClientWebSocketSession? = null
    var sessionId: String? = null

    private val _connectionState = MutableStateFlow<ConnectionState>(ConnectionState.NotConnected)
    val connectionState = _connectionState.asStateFlow()

    private val _authenticationState = MutableStateFlow<AuthenticationState>(AuthenticationState.Unauthorized)
    val authenticationState = _authenticationState.asStateFlow()

    /** Distinguishes the first connect from a reconnect; see the Incoming.Connected handler. */
    private var hasConnectedBefore = false

    // These maps are read/written from multiple coroutines and dispatchers (the receive loop,
    // every subscription/method channelFlow's awaitClose, the ping-pong loop). Plain mutableMapOf()
    // is not thread-safe: concurrent structural modification while the receive loop iterates can
    // throw ConcurrentModificationException and kill the loop, stopping all sync. ConcurrentMutableMap
    // serializes access across JVM and Native.
    //
    // The declared type is ConcurrentMutableMap, not MutableMap, on purpose: only the concrete type
    // exposes block{}. ConcurrentMutableMap locks per *operation*, so iterating `values`/`keys` takes
    // and releases the lock on every `next()` -- a concurrent put/remove mid-traversal still throws
    // ConcurrentModificationException from the backing LinkedHashMap. Any read that walks the whole
    // map, or any read-then-write pair that must be atomic, has to go through block{}.
    val subscriptions = ConcurrentMutableMap<String, Subscription>()

    val activeSubscriptions
        get() = subscriptions.block { map -> map.values.filter { it.state is SubscriptionState.Subscribed } }

    val subscriptionCount
        get() = activeSubscriptions.size


    private val subscriptionResultListeners = ConcurrentMutableMap<String, SubscriptionResultListener>()
    val methodResultListeners = ConcurrentMutableMap<String, MethodResultListener>()

    val pongListeners = ConcurrentMutableMap<String, PongListener>()

    private val restartChannel = Channel<Unit>(Channel.CONFLATED)
        .also { it.trySend(Unit) }

    private val refreshTokenChannel = Channel<Unit>(Channel.CONFLATED)
        .also { it.trySend(Unit) }

    // Buffered so a slow collector of ddpMessages cannot back-pressure (stall) the receive loop,
    // which would stop all incoming data from being processed. Oldest events are dropped under
    // pressure rather than blocking the socket.
    private val _ddpMessages = MutableSharedFlow<Incoming>(
        extraBufferCapacity = 64,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )
    val ddpMessages = _ddpMessages.asSharedFlow()

    @OptIn(ExperimentalCoroutinesApi::class)
    fun initConnection(): Flow<Incoming> = restartChannel
        .consumeAsFlow()
        .flatMapLatest {
            Logger.d(tag = "CONNECTION_RETRY") { "Restarting connection 1" }
            _connectionState.update {
                ConnectionState.Connecting(null)
            }
            flow {
                Logger.d(tag = "CONNECTION_RETRY") { "Starting webSocket connection" }
                try {
                    // webSocketSession (not webSocket(block)) so receiveAll runs in this flow
                    // collector's coroutine: ktor 3 runs webSocket's block inside the call's
                    // coroutine, and emit() from there violates the flow context invariant
                    // (process abort on iOS). webSocketSession pre-sets port to the WS default
                    // before the request block runs, so port must be re-derived after switching
                    // the protocol to WSS.
                    val session = httpClient.webSocketSession {
                        url.set(path = socketPath, host = host)
                        url.protocol = protocol
                        url.port = protocol.defaultPort
                    }
                    with(session) {
                        Logger.d(tag = "CONNECTION_RETRY") { "WebSocket connected, starting receiveAll" }
                        webSocketSession = this
                        try {
                            receiveAll {
                                val closed = handleIncoming(it)
                                emit(it)
                                // Emit first, then end the connection, so collectors actually
                                // observe Incoming.Close. The exception still drives retryWhen.
                                if (closed) throw ClosedReceiveChannelException("Unexpected close")

                                if (it is Incoming.Connected) {
                                    // A healthy connect resets the reconnect budget so a later
                                    // outage gets a fresh maxReconnectAttempts.
                                    connectionRetryCount = 0

                                    // Only a *re*connect needs restoring: on the first connect of
                                    // this client's life nothing was lost, and every subscription
                                    // in the map was created moments ago by a caller whose own
                                    // flow already sends its `sub`. Resubscribing them anyway went
                                    // through clearSubscriptions(), which wipes
                                    // subscriptionResultListeners -- so `ready` was then delivered
                                    // to the replacement listener and the caller's flow never
                                    // emitted Subscribed again. Any subscribe() racing this block
                                    // hit that.
                                    val isReconnect = hasConnectedBefore
                                    hasConnectedBefore = true
                                    if (pingPongEnabled) {
                                        launch {
                                            Logger.d(tag = "CONNECTION_RETRY") { "Starting ping pong" }
                                            startPingPong()
                                        }
                                    }
                                    // Run off the receive loop so a slow token refresh cannot
                                    // block processing of incoming messages. refreshToken() must
                                    // still complete before resubscribing, so they share one
                                    // coroutine and stay ordered.
                                    launch {
                                        // This runs in a launch{} whose failure bypasses the
                                        // connection flow's retryWhen/.catch and reaches the
                                        // unhandled coroutine handler -> app crash. A send on a
                                        // socket being torn down during a pause/background race
                                        // (e.g. ClosedSendChannelException) must degrade, not
                                        // crash; the receive loop / retry machinery reconnects.
                                        try {
                                            refreshToken()
                                            if (isReconnect) resubscribeAll()
                                        } catch (e: CancellationException) {
                                            throw e
                                        } catch (e: Exception) {
                                            Logger.d(tag = "CONNECTION_RETRY") {
                                                "refreshToken/resubscribe failed: ${e::class.simpleName} - ${e.message}"
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (e: Exception) {
                            Logger.d(tag = "CONNECTION_RETRY") { "receiveAll threw exception: ${e::class.simpleName} - ${e.message}" }
                            throw e
                        } finally {
                            Logger.d(tag = "CONNECTION_RETRY") { "receiveAll finished/cancelled" }
                            // The session is not a child of this collector (webSocketSession runs
                            // it on the client's scope), so it must be torn down explicitly when
                            // the flow is cancelled or restarted by flatMapLatest.
                            session.cancel()
                        }
                    }
                } catch (e: Exception) {
                    Logger.d(tag = "CONNECTION_RETRY") { "webSocket threw exception: ${e::class.simpleName} - ${e.message}" }
                    throw e
                }

            }.retryWhen { cause, _ ->
                if (!retryConnection) return@retryWhen false

                // Budget is per-outage (connectionRetryCount resets to 0 on each successful
                // Incoming.Connected), not the flow's lifetime like retryWhen's own `attempt`.
                val shouldRetry = connectionRetryCount < maxReconnectAttempts &&
                    cause.isHandled() && _connectionState.value != ConnectionState.Paused

                if (shouldRetry) {
                    val firstRetry = connectionRetryCount == 0
                    connectionRetryCount++
                    Logger.d(tag = "CONNECTION_RETRY") { "update connection state to connecting (retry $connectionRetryCount)" }
                    _connectionState.update {
                        ConnectionState.Connecting(cause)
                    }
                    if (!firstRetry) { // first retry is not delayed
                        delay(retryDelay)
                    }
                }

                shouldRetry
            }
            .catch { exception ->
                // Never rethrow: the only collector is a bare launchIn on the app's main scope
                // with no CoroutineExceptionHandler, so a rethrow becomes an unhandled coroutine
                // exception and aborts the process on Kotlin/Native (e.g. Ktor Darwin maps
                // NSURLErrorTimedOut to SocketTimeoutException — formerly not allowlisted — when
                // the network drops while idle). A connection failure of any type must degrade to
                // Disconnected; unexpected types are reported as non-fatals via Logger.e, which
                // routes through kermit's CrashlyticsLogWriter.
                if (!exception.isHandled()) {
                    Logger.e(
                        messageString = "* Unhandled connection exception, degrading to Disconnected",
                        throwable = exception,
                        tag = "CONNECTION_RETRY",
                    )
                }
                if (_connectionState.value != ConnectionState.Paused) {
                    emit(Incoming.Exception(exception))
                    _connectionState.update {
                        ConnectionState.Disconnected(exception as? Exception ?: RuntimeException(exception))
                    }
                    Logger.d(tag = "CONNECTION_RETRY") { "* Exception, closing connection $exception" }
                    closeConnection()
                }
            }
        }
        .onEach {
            _ddpMessages.emit(it)
        }
        .distinctUntilChanged()

    suspend fun CoroutineScope.refreshToken() {
        Logger.d(tag = "DDP_AUTH") { "Manual token refresh called authenticationState: ${authenticationState.value}" }
        // compareAndSet, not read-then-setAuthenticationState: the Authorized check and the flip to
        // Authorizing have to be one atomic step. When several in-flight calls get `unauthorized` at
        // once they all enter here concurrently, and with a separate read+write every one of them
        // saw Authorized and ran tokenRefresher -- N logins for one expiry.
        if (_authenticationState.compareAndSet(
                expect = AuthenticationState.Authorized,
                update = AuthenticationState.Authorizing,
            )
        ) {
            // Authorizing is a lock: while it is set, every other refreshToken() no-ops on the CAS
            // above and everything waiting on authenticationState (sendMessage, the re-auth flow in
            // startReAuthenticateFlow) is parked. It MUST be released on every exit path.
            //
            // refreshToken runs in a launch{} on the receive loop's scope, so the socket dropping
            // mid-refresh cancels it between the CAS and the write-back. Without this finally the
            // client stayed Authorizing forever: later refreshes no-opped, and sendMessage waited
            // for an Authorized/Unauthorized that could never arrive -- a permanent hang, not just
            // a slow reconnect. Falls back to Authorized because a cancelled refresh never learned
            // anything bad about the token; the next unauthorized response will try again.
            //
            // Assignment, not emit(): emit() suspends and would itself throw once cancelled.
            var newState: AuthenticationState = AuthenticationState.Authorized
            try {
                val success = async {
                    tokenRefresher()
                }.await()

                newState = if (success) {
                    Logger.d(tag = "DDP_AUTH") { "Automatic token refresh succeeded new state Authorized" }
                    AuthenticationState.Authorized
                } else {
                    Logger.d(tag = "DDP_AUTH") { "Automatic token refresh failed new state Unauthorized" }
                    AuthenticationState.Unauthorized
                }
            } finally {
                _authenticationState.value = newState
            }
        }
    }

    suspend fun restartConnection(): StateFlow<ConnectionState> {
        Logger.d(tag = "CONNECTION_RETRY") { "Restarting connection function connection state ${_connectionState.value}" }
        if (_connectionState.value != ConnectionState.Paused) {
            _connectionState.update {
                ConnectionState.Connecting(null)
            }
            // trySend, not send: restartChannel is CONFLATED so trySend never suspends, and unlike
            // send() it does not throw ClosedSendChannelException if the channel was already
            // closed/cancelled (e.g. a rapid foreground/background pause/resume race).
            restartChannel.trySend(Unit)
        }
        return _connectionState.asStateFlow()
    }

    suspend fun resumeConnection(): StateFlow<ConnectionState> {
        val isPaused = _connectionState.value == ConnectionState.Paused
        val isDisconnected = _connectionState.value is ConnectionState.Disconnected
        if (isPaused || isDisconnected) {
            Logger.d(tag = "CONNECTION_RETRY") { "Resume connection function connection state ${_connectionState.value}" }
            _connectionState.update {
                ConnectionState.Connecting(null)
            }
            // trySend, not send: restartChannel is CONFLATED so trySend never suspends, and unlike
            // send() it does not throw ClosedSendChannelException if the channel was already
            // closed/cancelled (e.g. a rapid foreground/background pause/resume race).
            restartChannel.trySend(Unit)
        }
        return _connectionState.asStateFlow()
    }

    /**
     * Reconnect from any non-connected state. Unlike [restartConnection], which is a no-op while
     * [ConnectionState.Paused], this resumes a paused connection. The method send/retry paths use it
     * so a call made during the brief foreground/background pause race can still recover instead of
     * hanging until timeout and failing permanently.
     */
    suspend fun reconnect(): StateFlow<ConnectionState> {
        return if (_connectionState.value == ConnectionState.Paused) {
            resumeConnection()
        } else {
            restartConnection()
        }
    }

    suspend fun closeConnection(paused: Boolean = false) {
        Logger.d(tag = "CONNECTION_RETRY") { "Closing connection $paused" }
        val exception = if (paused) {
            ClosedReceiveChannelException("Connection paused")
        } else {
            ClosedReceiveChannelException("Connection closed")
        }

        // Copy-then-clear must be ONE critical section: as two separate locked operations, a
        // concurrent put/remove landing in between invalidates the iterator mid-toList().
        val listeners = methodResultListeners.block { map ->
            map.values.toList().also { map.clear() }
        }

        // Now iterate over the copy
        listeners.forEach { listener ->
            listener.onConnectionClosed(exception)
        }

        //Cancel the WebSocket scope first to stop all child coroutines immediately
        webSocketSession?.cancel(cause = KotlinxCancellationException(exception.message ?: "Connection closed"))
        webSocketSession?.close(CloseReason(CloseReason.Codes.NORMAL, ""))

        webSocketSession = null
        val isNotDisconnected = _connectionState.value !is ConnectionState.Disconnected
        if (isNotDisconnected) {
            val newState = if (paused) ConnectionState.Paused else ConnectionState.NotConnected
            Logger.d(tag = "CONNECTION_RETRY") { "Update connection state to $newState" }
            _connectionState.update {
                newState
            }
        }
    }

    suspend fun pauseConnection() {
        Logger.d(tag = "CONNECTION_RETRY") { "Pause connection function connection state ${_connectionState.value}" }
        _connectionState.update {
            ConnectionState.Paused
        }
        closeConnection(true)
    }

    private suspend fun startPingPong() {
        Logger.d(tag = "CONNECTION_RETRY") { "Starting ping pong ${connectionState.value}" }
        try {
            while (connectionState.value == ConnectionState.DDPConnected) {
                val id = randomUUID()

                pongListeners[id] = object : PongListener {
                    override fun onPong(responseMessage: Incoming.Pong) {
                        // Handle pong response
                        pongListeners.remove(responseMessage.id) // Remove listener on pong response
                    }
                }

                val session = webSocketSession

                Logger.d(tag = "CONNECTION_RETRY") { "Trying to Ping session $session" }
                if (session != null) {
                    session.sendSerialized(Outgoing.Ping(id))

                    // Wait for pong response with a timeout
                    val pongReceived = withTimeoutOrNull(pingPongTimeout) {
                        while (pongListeners.containsKey(id)) {
                            delay(100)
                        }
                        true
                    }

                    if (pongReceived == null) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Pong not received in $pingPongTimeout ms" }
                        restartConnection()
                    }
                }

                delay(pingPongInterval) // Wait for interval before sending the next ping
            }

            Logger.d(tag = "CONNECTION_RETRY") { "Ping loop exited normally, state: ${connectionState.value}" }
        } catch (e: CancellationException) {
            Logger.d(tag = "CONNECTION_RETRY") { "Ping pong cancelled: ${e.message}, state: ${connectionState.value}" }
            throw e
        } catch (e: Exception) {
            // startPingPong runs in a launch{} whose failure bypasses the connection flow's
            // retryWhen/.catch, so re-throwing reached the unhandled coroutine handler and
            // crashed the app (Crashlytics: ClosedSendChannelException during a pause/background
            // race, where sendSerialized hits an already-closed socket). The keepalive is
            // best-effort: stop pinging and let the receive loop / retry machinery reconnect,
            // rather than make a dead socket fatal.
            Logger.d(tag = "CONNECTION_RETRY") { "Ping pong exception (stopping keepalive): ${e::class.simpleName} - ${e.message}" }
        } finally {
            Logger.d(tag = "CONNECTION_RETRY") { "Exit from ping pong finally block, state: ${connectionState.value}" }
        }
    }

    fun setAuthenticationState(authenticationState: AuthenticationState) {
        Logger.d(tag = "CONNECTION_RETRY") { "Updating authentication state to $authenticationState" }
        _authenticationState.update {
            authenticationState
        }
    }

    fun subscribe(
        name: String,
        params: JsonArray? = null,
        subscriptionId: String? = null
    ) = subscriptionFlow(
        subscription = subscriptionId?.let {
            Subscription(id = it, name = name, params = params)
        } ?: Subscription(name = name, params = params),
        onEach = { subscription ->
            subscriptions[subscription.id] = subscription
        
        }
    ) { subscription ->
        subscriptions[subscription.id] = subscription

        // ✅ Use StateFlow instead of mutable variables
        val subState = MutableStateFlow(SubscriptionCallState())
        var tokenRefresherJob: Job? = null

        subscriptionResultListeners.getOrPut(subscription.id) {
            createSubscriptionListener(
                subscription = subscription,
                name = name,
                params = params,
                subState = subState,
                onLaunchTokenRefresh = { job -> tokenRefresherJob = job }
            )
        }

        sendMessageAndHandleResult(subscription.subscriptionMessage)

        awaitClose {
            Logger.d(tag = "CONNECTION_RETRY") { "awaitClose: Closing subscription for ${subscription.id}" }
            tokenRefresherJob?.cancel()
            subscriptionResultListeners.remove(subscription.id)
        }
    }

    // State holder for subscription
    data class SubscriptionCallState(
        val cancelSendingSuccess: Boolean = false,
        val finished: Boolean = false,
        val receivedReady: Boolean = false,
        val resent: Boolean = false
    )

    // Extract listener creation to separate function
    fun ProducerScope<SubscriptionState>.createSubscriptionListener(
        subscription: Subscription,
        name: String,
        params: JsonArray?,
        subState: MutableStateFlow<SubscriptionCallState>,
        onLaunchTokenRefresh: (Job) -> Unit
    ): SubscriptionResultListener = object : SubscriptionResultListener {

        override suspend fun onReady(subId: String) {
            val state = subState.value
            if (!state.cancelSendingSuccess) {
                if (!state.receivedReady) {

                    send(SubscriptionState.Subscribed)
                }
                subState.update { it.copy(receivedReady = true) }
            }
        }

        override suspend fun onNoSub(responseMessage: Incoming.NoSub) {
            Logger.d(tag = "CONNECTION_RETRY") { "onNoSub: $responseMessage" }
            subState.update { it.copy(cancelSendingSuccess = true) }

            val state = subState.value
            val shouldRetry = responseMessage.error != null
                    && unauthorizedChecker(name, params, responseMessage.error)
                    && !state.resent
                    && !state.finished

            Logger.d(tag = "CONNECTION_RETRY") { "shouldRetry: $shouldRetry state: $state responseMessage $responseMessage" }

            if (shouldRetry) {
                handleSubscriptionRetry(
                    subscription = subscription,
                    error = responseMessage.error,
                    subState = subState,
                    onLaunchTokenRefresh = onLaunchTokenRefresh
                )
            } else {
                handleSubscriptionError(responseMessage.error, subState)
            }
        }

        private fun handleSubscriptionRetry(
            subscription: Subscription,
            error: ResponseError?,
            subState: MutableStateFlow<SubscriptionCallState>,
            onLaunchTokenRefresh: (Job) -> Unit
        ) {
            val job = launch {
                refreshToken()
                authenticationState.collect { authState ->
                    Logger.d(tag = "CONNECTION_RETRY") { "handleSubscriptionRetry: $authState ${subState.value}" }
                    val currentState = subState.value
                    if (currentState.finished) return@collect

                    if (authState == AuthenticationState.Authorized) {
                        if (!currentState.resent) {
                            subState.update {
                                it.copy(
                                    cancelSendingSuccess = false,
                                    receivedReady = false,
                                    resent = true
                                )
                            }
                            sendMessageAndHandleResult(subscription.subscriptionMessage)
                        }
                    }
                    else if (authState == AuthenticationState.Unauthorized) {
                        handleSubscriptionError(error, subState)
                    }
                }
            }
            onLaunchTokenRefresh(job)
        }

        private suspend fun handleSubscriptionError(
            error: ResponseError?,
            subState: MutableStateFlow<SubscriptionCallState>
        ) {
            if (!subState.value.finished) {
                subState.update { it.copy(finished = true) }
                send(SubscriptionState.Error(error))
                close()
            }
        }
    }

    fun ProducerScope<SubscriptionState>.sendMessageAndHandleResult(
        outgoing: Outgoing,
    ) = launch {
        sendMessage(outgoing).collect { messageState ->
            when (messageState) {
                is MessageState.Success -> {
                    if (outgoing is Outgoing.Unsubscribe) {
                        send(SubscriptionState.Unsubscribing)
                    } else if (outgoing is Outgoing.Subscribe) {
                        send(SubscriptionState.Subscribing)
                    }
                }
                is MessageState.Failed -> {
                    send(SubscriptionState.Error(null))
                    close() // Close the channelFlow when a failure occurs
                }
            }
        }
    }

    fun unsubscribe(id: String): Flow<SubscriptionState> = channelFlow {
        val unsubscribe = Outgoing.Unsubscribe(id)

        subscriptionResultListeners[id] =
            object : UnSubscriptionResultListener() {
                override suspend fun onNoSub(responseMessage: Incoming.NoSub) {
                    send(SubscriptionState.Unsubscribed)
                    close()
                }
            }

        sendMessageAndHandleResult(unsubscribe)

        awaitClose {
            subscriptionResultListeners.remove(id)
        }
    }
    .onEach {
        val subscription = subscriptions[id] ?: return@onEach
        subscriptions[id] = subscription.copy(state = it)
    }

    fun unsubscribeFromAll() = unsubscribeFrom { true }

    fun unsubscribeFrom(filter: (Subscription) -> Boolean): Flow<UnsubscribeAllState> {
        val subscriptions = activeSubscriptions.filter(filter)

        return if (subscriptions.isEmpty()) {
            flowOf(UnsubscribeAllState.UnsubscribedAll)
        } else {
            unsubscribeFrom(subscriptions.map { it.id })
        }
    }

    private fun unsubscribeFrom(ids: List<String>) = channelFlow {
        ids.map { unsubscribe(it) }
            .let { flowList ->
                flowList.combineStates()
                    .distinctUntilChanged()
                    .collect { send(it) }
            }
    }

    private fun CoroutineScope.resubscribeAll() {
        // Re-subscribe to everything that was active OR still in-flight (Subscribing) when the
        // connection dropped. Previously only `activeSubscriptions` (Subscribed) were restored,
        // so a subscription that had not yet received its `ready` was lost forever and its screen
        // never synced after a reconnect.
        val subscriptionsToResubscribe = subscriptions.block { map ->
            map.values.filter {
                it.state is SubscriptionState.Subscribed || it.state is SubscriptionState.Subscribing
            }
        }
        clearSubscriptions()

        // Resubscribe reusing the same IDs
        for (subscription in subscriptionsToResubscribe) {
            subscribe(
                name = subscription.name,
                params = subscription.params,
                subscriptionId = subscription.id,
            ).launchIn(this)
        }
    }

    fun dropDb() {
        db.drop()
    }

    fun clearSubscriptions() {
        subscriptions.clear()
        subscriptionResultListeners.clear()
    }

    // Define CallState outside the function to avoid bytecode issues
    sealed class CallState {
        object Active : CallState()
        object Finished : CallState()
        data class Resending(val retryCount: Int = 0) : CallState()
    }

    inline fun <reified T : Any> call(method: String, params: JsonArray? = null, randomSeed: String? = null) : Flow<MethodState<T>> {
        val id = randomUUID()
        val methodMessage = Outgoing.Method(method, params, id, randomSeed)

        val callState = MutableStateFlow<CallState>(CallState.Active)
        var tokenRefresherJob: Job? = null

        return channelFlow {
            send(MethodState.Loading)

            methodResultListeners[id] = createMethodResultListener(
                callState = callState,
                methodMessage = methodMessage,
                onLaunchTokenRefresh = { job -> tokenRefresherJob = job }
            )

            // Wrap initial call in try-catch
            try {
                sendMessageAndHandleResult<T>(methodMessage, callState)
            } catch (e: Exception) {
                Logger.d(tag = "CONNECTION_RETRY") { "Exception in call function: ${e.message}" }
                // Call onException for any uncaught exceptions
                methodResultListeners[id]?.onException(e)
            }

            awaitClose {
                tokenRefresherJob?.cancel()
                methodResultListeners.remove(id)
            }
        }
    }

    inline fun <reified T : Any> ProducerScope<MethodState<T>>.createMethodResultListener(
        callState: MutableStateFlow<CallState>,
        methodMessage: Outgoing.Method,
        crossinline onLaunchTokenRefresh: (Job) -> Unit
    ): MethodResultListener {
        return object : MethodResultListener {
            override suspend fun onResult(responseMessage: Incoming.Result) {
                handleResult(
                    responseMessage,
                    callState,
                    methodMessage,
                    onLaunchTokenRefresh
                )
            }

            override suspend fun onUpdated(responseMessage: Incoming.Updated) {
                if (callState.value != CallState.Finished) {
                    send(MethodState.Updated)
                } else {
                    close()
                }
            }

            override suspend fun onException(exception: Exception) {
                Logger.d(tag = "CONNECTION_RETRY") { "Exception occurred: ${exception.message}" }

                val currentState = callState.value
                val retryCount = if (currentState is CallState.Resending) {
                    currentState.retryCount + 1
                } else {
                    1
                }

                Logger.d(tag = "CONNECTION_RETRY") { "Exception for method ${methodMessage.method}, retry count: $retryCount" }

                // Only retry once on exception
                if (retryCount >= 2) {
                    if (callState.value != CallState.Finished) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Exception for method ${methodMessage.method}, retry count: $retryCount setting state to finished" }
                        send(MethodState.Exception(exception))
                        callState.value = CallState.Finished
                        close()
                    }
                    return
                }

                // Update state before checking connection
                callState.value = CallState.Resending(retryCount)

                // Check current connection state before trying to ping
                val currentConnectionState = connectionState.value
                Logger.d(tag = "CONNECTION_RETRY") { "Current connection state before check: $currentConnectionState" }

                // If already disconnected or connecting, skip ping check and just reconnect
                if (currentConnectionState is ConnectionState.Disconnected ||
                    currentConnectionState is ConnectionState.Connecting ||
                    currentConnectionState == ConnectionState.NotConnected ||
                    currentConnectionState == ConnectionState.Paused) {
                    Logger.d(tag = "CONNECTION_RETRY") { "Connection state indicates disconnection, skipping ping check" }

                    // Paused must be resumed here; restartConnection() is a no-op while Paused,
                    // which would leave this call hanging until timeout. reconnect() resumes instead.
                    reconnect()

                    val reconnected = withTimeoutOrNull(timeout) {
                        connectionState.first { it == ConnectionState.DDPConnected }
                        true
                    } ?: false

                    if (!reconnected) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Failed to reconnect within timeout for method ${methodMessage.method} setting state to finished" }
                        send(MethodState.Exception(exception))
                        callState.value = CallState.Finished
                        close()
                        return
                    }

                    Logger.d(tag = "CONNECTION_RETRY") { "Reconnected after exception, retrying method ${methodMessage.method}" }

                    if (callState.value !is CallState.Finished) {
                        sendMessageAndHandleResult<T>(methodMessage, callState)
                    }
                    return
                }

                // Connection state looks good, try ping check
                var pongReceived = false
                var connectionCheckFailed = false
                val pongCheckId = randomUUID()

                pongListeners[pongCheckId] = object : PongListener {
                    override fun onPong(responseMessage: Incoming.Pong) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Pong received for connection check after exception" }
                        pongReceived = true
                        pongListeners.remove(responseMessage.id)
                    }
                }

                try {
                    val session = webSocketSession
                    if (session == null) {
                        Logger.d(tag = "CONNECTION_RETRY") { "WebSocket session is null during connection check" }
                        connectionCheckFailed = true
                    } else {
                        session.sendSerialized(Outgoing.Ping(pongCheckId))

                        withTimeoutOrNull(pingPongTimeout) {
                            while (!pongReceived) {
                                delay(50)
                            }
                        }
                    }
                } catch (e: Exception) {
                    Logger.d(tag = "CONNECTION_RETRY") { "Exception during connection check: ${e.message}" }
                    connectionCheckFailed = true
                } finally {
                    pongListeners.remove(pongCheckId)
                }

                Logger.d(tag = "CONNECTION_RETRY") { "Connection check result - pong received: $pongReceived, checkFailed: $connectionCheckFailed, state: ${connectionState.value}" }

                if (callState.value is CallState.Finished) {
                    return
                }

                if (pongReceived) {
                    // Connection is fine, but exception occurred - this is a real error
                    Logger.d(tag = "CONNECTION_RETRY") { "Connection is fine, but exception occurred - this is a real error setting state to finished" }
                    send(MethodState.Exception(exception))
                    callState.value = CallState.Finished
                    close()
                } else if (connectionCheckFailed || !pongReceived) {
                    // Connection failed, restart and retry
                    Logger.d(tag = "CONNECTION_RETRY") { "Connection failed after exception, restarting for method ${methodMessage.method}" }

                    restartConnection()

                    // Wait for connection to be established with timeout
                    val reconnected = withTimeoutOrNull(timeout) {
                        connectionState.first { it == ConnectionState.DDPConnected }
                        true
                    } ?: false

                    if (!reconnected) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Failed to reconnect within timeout for method ${methodMessage.method} setting state to finished" }
                        send(MethodState.Exception(exception))
                        callState.value = CallState.Finished
                        close()
                        return
                    }

                    Logger.d(tag = "CONNECTION_RETRY") { "Reconnected after exception, retrying method ${methodMessage.method}" }

                    if (callState.value !is CallState.Finished) {
                        sendMessageAndHandleResult<T>(methodMessage, callState)
                    }
                }
            }

            override suspend fun onConnectionClosed(exception: Exception) {
                Logger.d(tag = "CONNECTION_RETRY") { "Connection closed: ${exception.message}" }
                // Connection was deliberately closed, just finish without retry
                if (callState.value != CallState.Finished) {
                    Logger.d(tag = "CONNECTION_RETRY") { "Connection closed: ${exception.message} setting state to finished" }
                    send(MethodState.Exception(exception))
                    callState.value = CallState.Finished
                    close()
                }
            }

            override suspend fun onTimeout() {
                val currentState = callState.value
                val timeoutCount = if (currentState is CallState.Resending) {
                    currentState.retryCount + 1
                } else {
                    1
                }

                Logger.d(tag = "CONNECTION_RETRY") { "Timeout occurred for method ${methodMessage.method}, count: $timeoutCount, current state: $currentState" }

                // Only send exception and finish if this is the second timeout
                if (timeoutCount >= 2) {
                    Logger.d(tag = "CONNECTION_RETRY") { "Timeout occurred for method setting state to finished" }
                    send(MethodState.Exception(TimeoutException(timeout)))
                    callState.value = CallState.Finished
                    close()
                    return
                }

                // First timeout: Update state BEFORE checking connection
                callState.value = CallState.Resending(timeoutCount)

                // Check current connection state before trying to ping
                val currentConnectionState = connectionState.value
                Logger.d(tag = "CONNECTION_RETRY") { "Current connection state before check: $currentConnectionState" }

                // If already disconnected or connecting, skip ping check and just reconnect
                if (currentConnectionState is ConnectionState.Disconnected ||
                    currentConnectionState is ConnectionState.Connecting ||
                    currentConnectionState == ConnectionState.NotConnected ||
                    currentConnectionState == ConnectionState.Paused) {
                    Logger.d(tag = "CONNECTION_RETRY") { "Connection state indicates disconnection, skipping ping check" }

                    // Paused must be resumed here; restartConnection() is a no-op while Paused,
                    // which would leave this call hanging until timeout. reconnect() resumes instead.
                    reconnect()

                    val reconnected = withTimeoutOrNull(timeout) {
                        connectionState.first { it == ConnectionState.DDPConnected }
                        true
                    } ?: false

                    if (!reconnected) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Failed to reconnect within timeout for method ${methodMessage.method} setting state to finished" }
                        send(MethodState.Exception(TimeoutException(timeout)))
                        callState.value = CallState.Finished
                        close()
                        return
                    }

                    Logger.d(tag = "CONNECTION_RETRY") { "Reconnected, retrying method ${methodMessage.method}" }

                    if (callState.value !is CallState.Finished) {
                        sendMessageAndHandleResult<T>(methodMessage, callState)
                    }
                    return
                }

                // Connection state looks good, try ping check
                var pongReceived = false
                var connectionCheckFailed = false
                val pongCheckId = randomUUID()

                // Create a listener to track if pong is received
                pongListeners[pongCheckId] = object : PongListener {
                    override fun onPong(responseMessage: Incoming.Pong) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Pong received for connection check" }
                        pongReceived = true
                        pongListeners.remove(responseMessage.id)
                    }
                }

                try {
                    val session = webSocketSession
                    if (session == null) {
                        Logger.d(tag = "CONNECTION_RETRY") { "WebSocket session is null during connection check" }
                        connectionCheckFailed = true
                    } else {
                        session.sendSerialized(Outgoing.Ping(pongCheckId))

                        // Wait for pong with timeout
                        withTimeoutOrNull(pingPongTimeout) {
                            while (!pongReceived) {
                                delay(50)
                            }
                        }
                    }
                } catch (e: Exception) {
                    Logger.d(tag = "CONNECTION_RETRY") { "Exception during connection check: ${e.message}" }
                    connectionCheckFailed = true
                } finally {
                    pongListeners.remove(pongCheckId)
                }

                Logger.d(tag = "CONNECTION_RETRY") { "Connection check result - pong received: $pongReceived, checkFailed: $connectionCheckFailed, state: ${connectionState.value}" }

                if (callState.value is CallState.Finished) {
                    // Already finished, don't retry
                    return
                }

                if (pongReceived) {
                    // Connection is fine, but server didn't respond - this is a real timeout
                    // Throw timeout exception immediately
                    Logger.d(tag = "CONNECTION_RETRY") { "Connection is fine, but server didn't respond - this is a real timeout setting state to finished" }
                    send(MethodState.Exception(TimeoutException(timeout)))
                    callState.value = CallState.Finished
                    close()
                } else if (connectionCheckFailed || !pongReceived) {
                    // Connection failed, restart and retry
                    Logger.d(tag = "CONNECTION_RETRY") { "Connection failed, restarting for method ${methodMessage.method}" }

                    restartConnection()

                    // Wait for connection to be established with timeout
                    val reconnected = withTimeoutOrNull(timeout) {
                        connectionState.first { it == ConnectionState.DDPConnected }
                        true
                    } ?: false

                    if (!reconnected) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Failed to reconnect within timeout for method ${methodMessage.method} setting state to finished" }
                        send(MethodState.Exception(TimeoutException(timeout)))
                        callState.value = CallState.Finished
                        close()
                        return
                    }

                    Logger.d(tag = "CONNECTION_RETRY") { "Reconnected, retrying method ${methodMessage.method}" }

                    if (callState.value !is CallState.Finished) {
                        sendMessageAndHandleResult<T>(methodMessage, callState)
                    }
                }
            }
        }
    }


    suspend inline fun <reified T : Any> ProducerScope<MethodState<T>>.handleResult(
        responseMessage: Incoming.Result,
        callState: MutableStateFlow<CallState>,
        methodMessage: Outgoing.Method,
        crossinline onLaunchTokenRefresh: (Job) -> Unit
    ) {
        val error = responseMessage.error

        if (error != null) {
            val shouldRetry = unauthorizedChecker(methodMessage.method, methodMessage.params, error)
            val currentState = callState.value
            val canRetry = currentState is CallState.Active

            if (shouldRetry && canRetry) {
                startReAuthenticateFlow(
                    callState,
                    methodMessage,
                    error,
                    onLaunchTokenRefresh
                )
            } else {
                Logger.d(tag = "CONNECTION_RETRY") { "Error received for method ${methodMessage.method}, setting state to finished" }
                send(MethodState.Error(error))
                callState.value = CallState.Finished
                close()
            }
        }
        else {
            val resultObject = responseMessage.result?.let {
                json.decodeFromJsonElement<T>(it)
            }
            Logger.d(tag = "CONNECTION_RETRY") { "Result received for method ${methodMessage.method}, setting state to finished" }
            send(MethodState.Success(resultObject))
            callState.value = CallState.Finished
            close()
        }
    }

    inline fun <reified T : Any> ProducerScope<MethodState<T>>.startReAuthenticateFlow(
        callState: MutableStateFlow<CallState>,
        methodMessage: Outgoing.Method,
        error: ResponseError,
        crossinline onLaunchTokenRefresh: (Job) -> Unit
    ) {
        val tokenRefresherJob = launch {
            refreshToken()
            authenticationState
                .takeWhile { callState.value != CallState.Finished }
                .collect {
                    if (it == AuthenticationState.Authorized) {
                        sendMessageAndHandleResult<T>(methodMessage, callState)
                    } else if (it == AuthenticationState.Unauthorized) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Unauthorized for method ${methodMessage.method}, setting state to finished" }
                        send(MethodState.Error(error))
                        callState.value = CallState.Finished
                        close()
                    }
                }
        }

        onLaunchTokenRefresh(tokenRefresherJob)
        callState.value = CallState.Resending(0)
    }

    inline fun <reified T : Any> ProducerScope<MethodState<T>>.sendMessageAndHandleResult(
        outgoing: Outgoing.Method,
        callState: MutableStateFlow<CallState>
    ) = launch {
        Logger.d(tag = "CONNECTION_RETRY") { "Sending message ${outgoing.method}, current state: ${callState.value}" }

        // Start timeout for this specific call
        val timeoutJob = launch {
            val result = withTimeoutOrNull(timeout) {
                callState.first { it == CallState.Finished }
            }

            // If result is null, timeout occurred
            if (result == null && callState.value != CallState.Finished) {
                Logger.d(tag = "CONNECTION_RETRY") { "Timeout detected for method ${outgoing.method}, state before timeout: ${callState.value}" }
                methodResultListeners[outgoing.id]?.onTimeout()
            }
        }

        try {
            sendMessage(outgoing).collect { messageState ->
                when (messageState) {
                    is MessageState.Success -> {
                        Logger.d(tag = "CONNECTION_RETRY") { "Message sent successfully ${outgoing.method}" }
                        // Keep the current state - don't reset to Active
                        // The timeout counter should be preserved in Resending state
                    }
                    is MessageState.Failed -> {
                        Logger.d(tag = "CONNECTION_RETRY") { "Failed to send message ${outgoing.method} exception ${messageState.exception}" }
                        timeoutJob.cancel()

                        // Call onException to handle the failure
                        if (callState.value !is CallState.Finished) {
                            val exception = messageState.exception ?: Exception("Failed to send message")
                            methodResultListeners[outgoing.id]?.onException(exception)
                        }
                    }
                }
            }
        } catch (e: Exception) {
            Logger.d(tag = "CONNECTION_RETRY") { "Exception while sending message ${outgoing.method}: ${e.message}" }
            timeoutJob.cancel()

            // Check if this is a cancellation exception
            if (e is CancellationException) {
                // If we're already finished, this cancellation is expected (flow cleanup)
                if (callState.value == CallState.Finished) {
                    Logger.d(tag = "CONNECTION_RETRY") { "Coroutine cancelled for ${outgoing.method} after completion - this is expected" }
                    return@launch
                }

                // If we're not finished yet, this is an unexpected cancellation
                // Treat it as a connection error and trigger retry logic
                Logger.d(tag = "CONNECTION_RETRY") { "Unexpected cancellation for ${outgoing.method} - treating as connection error" }
                methodResultListeners[outgoing.id]?.onException(Exception("Channel was cancelled unexpectedly", e))
                return@launch
            }

            // Call onException to handle other exceptions
            if (callState.value != CallState.Finished) {
                methodResultListeners[outgoing.id]?.onException(e)
            }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun sendMessage(outgoing: Outgoing): Flow<MessageState> = flow {
        Logger.d(tag = "CONNECTION_RETRY") { "Sending message 5 ${outgoing} connection state ${connectionState.value}" }
        if (connectionState.value != ConnectionState.DDPConnected && connectionState.value !is ConnectionState.Connecting && connectionState.value != ConnectionState.Paused) {
            Logger.d(tag = "CONNECTION_RETRY") { "Restarting connection 2" }
            restartConnection()
                .flatMapLatest {
                    when (it) {
                        ConnectionState.DDPConnected -> {
                            Logger.d(tag = "CONNECTION_RETRY") { "Connection restarted refreshing token" }
                            coroutineScope {
                                refreshToken()
                            }
                            authenticationState
                        }
                        is ConnectionState.Disconnected -> {
                            Logger.d(tag = "CONNECTION_RETRY") { "Connection disconnected emitting Unauthorized" }
                            flowOf(AuthenticationState.Unauthorized)
                        }
                        else -> {
                            flowOf(null)
                        }
                    }
                }
                .filterNotNull()
                // transformWhile, not collect: authenticationState is a StateFlow and never
                // completes, so a plain collect stayed subscribed after the message went out and
                // fired again on the next Authorized emission -- re-sending the same message and
                // emitting a second MessageState.Success. sendMessage delivers one message and
                // then finishes, so the collection has to stop at the first terminal outcome.
                .transformWhile { authState ->
                    Logger.d(tag = "CONNECTION_RETRY") { "Authentication state changed ${authState}" }
                    val session = webSocketSession

                    when {
                        authState == AuthenticationState.Authorized && session != null -> {
                            Logger.d(tag = "CONNECTION_RETRY") { "Sending message 2 ${outgoing} connection state ${connectionState.value}" }
                            session.sendSerialized(outgoing)
                            emit(MessageState.Success)
                            false
                        }
                        authState == AuthenticationState.Unauthorized -> {
                            emit(MessageState.Failed(null)) //TODO: add exception
                            false
                        }
                        // Authorizing, or Authorized before the session exists: keep waiting.
                        else -> true
                    }
                }
                .collect { emit(it) }
        } else if (connectionState.value is ConnectionState.Connecting) {
            // Same reason as the branch above: connectionState is a StateFlow, so collecting it
            // outlived the send and re-sent on the next DDPConnected (after any later drop).
            connectionState
                .transformWhile {
                    Logger.d(tag = "CONNECTION_RETRY") { "Sending message 3 connection state changed" }
                    if (it is ConnectionState.Connecting) return@transformWhile true

                    val connected = it is ConnectionState.DDPConnected

                    val session = webSocketSession
                    Logger.d(tag = "CONNECTION_RETRY") { "Sending message 3 session $session connection state ${connectionState.value}" }
                    if (connected && session != null) {
                        Logger.d(tag = "CONNECTION_RETRY") { "Sending message 3 ${outgoing} connection state ${connectionState.value}" }
                        session.sendSerialized(outgoing)
                        emit(MessageState.Success)
                    } else if (it is ConnectionState.Disconnected) {
                        emit(MessageState.Failed(it.exception))
                    } else {
                        emit(MessageState.Failed(null)) //TODO: add exception
                    }
                    false
                }
                .collect { emit(it) }
        } else {
            val session = webSocketSession

            if (session != null) {
                Logger.d(tag = "CONNECTION_RETRY") { "Sending message 4 ${outgoing} connection state ${connectionState.value}" }
                session.sendSerialized(outgoing)
                emit(MessageState.Success)
            } else {
                emit(MessageState.Failed(null))
            }
        }
    }

    /** Returns true when the message ends the connection, so the caller can emit it and then throw. */
    private suspend fun DefaultClientWebSocketSession.handleIncoming(incoming: Incoming): Boolean {
        when (incoming) {
            Incoming.Open -> {
                connect(
                    session = sessionId,
                    version = ddpVersion,
                    support = SUPPORTED_DDP_VERSIONS.toList(),
                )
            }
            is Incoming.Ping -> {
                sendSerialized(Outgoing.Pong(incoming.id))
            }
            is Incoming.Failed -> {
                val desiredVersion = incoming.version

                if (isVersionSupported(desiredVersion)) {
                    ddpVersion = desiredVersion
                } else {
                    throw RuntimeException("Protocol version not supported: $desiredVersion")
                }
            }
            is Incoming.Connected -> {
                sessionId = incoming.session
                _connectionState.update {
                    ConnectionState.DDPConnected
                }
            }
            is Incoming.Added -> {
                db.onDataAdded(incoming.collection, incoming.id, incoming.fields)
            }
            is Incoming.Changed -> {
                db.onDataChanged(incoming.collection, incoming.id, incoming.fields, incoming.cleared)
            }
            is Incoming.Removed -> {
                db.onDataRemoved(incoming.collection, incoming.id)
            }
            is Incoming.Ready -> {
                for (id in incoming.subs) {
                    subscriptionResultListeners[id]?.onReady(id)
                }
            }
            is Incoming.NoSub -> {
                Logger.d(tag = "CONNECTION_RETRY") { "NoSub received, closing connection for ${subscriptionResultListeners[incoming.id]}" }
                subscriptionResultListeners[incoming.id]?.onNoSub(incoming)
            }
            is Incoming.Result -> {
                methodResultListeners[incoming.id]?.onResult(incoming)
            }
            Incoming.Close -> {
                Logger.d(tag = "CONNECTION_RETRY") { "Close received, closing connection" }
                closeConnection()
                // Do NOT throw here: handleIncoming runs *before* emit(), so throwing made
                // Incoming.Close unreachable for every collector -- a public sealed member that
                // could never be observed. The caller rethrows after emitting, which keeps the
                // ClosedReceiveChannelException that drives retryWhen's reconnect.
                return true
            }
            is Incoming.Exception -> {
                throw incoming.exception
            }
            is Incoming.Updated -> {
                for (id in incoming.methods) {
                    methodResultListeners[id]?.onUpdated(incoming)
                }
            }
            is Incoming.Error,
            Incoming.Heartbeat,
            Incoming.Message -> return false
            is Incoming.Pong -> {
                pongListeners[incoming.id]?.onPong(incoming)
            }

        }
        return false
    }

    private suspend fun DefaultClientWebSocketSession.connect(
        session: String? = null,
        version: String,
        support: List<String>,
    ) {
        val connectMessage =
            Outgoing.Connect(session = session, version = version, support = support)

        sendSerialized(connectMessage)
    }

    /**
     * Returns whether the specified version of the DDP protocol is supported or not
     *
     * @param protocolVersion the DDP protocol version
     * @return whether the version is supported or not
     */
    private fun isVersionSupported(protocolVersion: String?) =
        SUPPORTED_DDP_VERSIONS.contains(protocolVersion)

    companion object {
        val SUPPORTED_DDP_VERSIONS = arrayOf("1", "pre1", "pre2")
    }
}

sealed class MessageState {
    data object Success : MessageState()
    data class Failed(val exception: Exception?) : MessageState()
}

sealed class ConnectionState {
    data class Disconnected(val exception: Exception) : ConnectionState()
    data class Connecting(val exception: Throwable?) : ConnectionState()
    data object NotConnected : ConnectionState()
    data object DDPConnected : ConnectionState()
    data object Paused : ConnectionState()
}

sealed class AuthenticationState {
    data object Authorized : AuthenticationState()
    data object Authorizing : AuthenticationState()
    data object Unauthorized : AuthenticationState()
}

class TimeoutException(val timeout: Long) : Exception("Request timeout after ${timeout}ms")
