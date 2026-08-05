# ddpclient

[![Maven Central](https://img.shields.io/maven-central/v/io.bordo/ddpclient)](https://central.sonatype.com/artifact/io.bordo/ddpclient)
[![CI](https://github.com/bordoio/kmp-ddpclient/actions/workflows/ci.yml/badge.svg)](https://github.com/bordoio/kmp-ddpclient/actions/workflows/ci.yml)

A Kotlin Multiplatform client for [Meteor](https://www.meteor.com/)'s DDP protocol over SockJS
websockets: connection management with reconnect, subscriptions backed by an in-memory minimongo
mirror, and method calls exposed as coroutine `Flow`s.

Targets: **Android**, **iosArm64**, **iosSimulatorArm64**, **iosX64**.

## Contents

- [Install](#install)
- [Quickstart](#quickstart)
- [How it works](#how-it-works)
- [Configuration reference](#configuration-reference)
- [Connection lifecycle](#connection-lifecycle)
- [Reconnect and retry](#reconnect-and-retry)
- [Keepalive (ping/pong)](#keepalive-pingpong)
- [Authentication](#authentication)
- [Method calls](#method-calls)
  - [Predictable ids (`randomSeed`)](#predictable-ids-randomseed)
- [Subscriptions](#subscriptions)
- [Collections](#collections)
- [Implementing your own Database](#implementing-your-own-database)
- [EJSON](#ejson)
- [Concurrency notes](#concurrency-notes)
- [Building and testing](#building-and-testing)
- [Known limitations](#known-limitations)

## Install

```kotlin
// build.gradle.kts
kotlin {
    sourceSets {
        commonMain.dependencies {
            implementation("io.bordo:ddpclient:0.1.2")
        }
    }
}
```

`io.bordo:ddpclient-ejson` is published separately for the EJSON surrogates (`$date`, `$binary`,
`$regexp`) if you need them without the client. The client already exposes it as an `api`
dependency, so you do not need to add it explicitly.

## Quickstart

```kotlin
val client = DDPClient("example.meteor.app") {
    // every setting is optional; these are the defaults
    protocol = URLProtocol.WSS
    retryConnection = true
    maxReconnectAttempts = 3
    timeout = 20_000
}

// Collecting the connection flow starts it. It never throws: every failure arrives as
// Incoming.Exception and the client moves to ConnectionState.Disconnected.
scope.launch { client.initConnection().collect() }

// Methods are cold Flows of MethodState
client.call<User>("users.get", buildJsonArray { add("someId") })
    .collect { state ->
        when {
            state.loading   -> showSpinner()
            state.succeeded -> render(state.content!!)
            state.failed    -> showError(state.error)
        }
    }

// Subscriptions are cold too. Collect or launch them, otherwise nothing is sent.
client.subscribe("users").launch(scope)

// The subscription feeds the local database; observe the collection separately.
client.db.receiveCollection<User>("users").collect { users -> render(users) }
```

Everything the client exposes is a **cold** `Flow`. `initConnection()`, `call()`, `subscribe()` and
`unsubscribe()` do nothing until collected, and cancelling the collection is how you tear the
corresponding thing down — a cancelled `subscribe()` removes its listener, a cancelled `call()`
drops its pending result.

## How it works

```
your code
   │  call() / subscribe()            receiveCollection()
   ▼                                          ▲
DDPClient ──── Outgoing ──► DDPMessageConverter ──► SockJS frame ──► server
   ▲                                                                  │
   └──── Incoming ◄── DDPMessageConverter ◄── SockJS frame ◄──────────┘
   │
   ├─► listener maps (method / subscription / pong)  → resolves your Flows
   └─► Database (minimongo mirror)                   → feeds collection Flows
```

**Transport.** A ktor `HttpClient` with the `WebSockets` plugin, connected to
`wss://<host>/<socketPath>`. `socketPath` defaults to a freshly generated SockJS path of the form
`sockjs/<serverId>/<sessionId>/websocket`, which is what a Meteor server expects.

**Framing.** `DDPMessageConverter` handles the SockJS envelope. Outgoing DDP messages are JSON-encoded
and then wrapped in a JSON array (SockJS's format). Incoming frames are dispatched by their first
character — `o` open, `c` close, `h` heartbeat, `m` message, `a[...]` a batch of DDP payloads — and
the payload is then decoded to a concrete `Incoming` subtype by its `msg` field. An unrecognised
`msg` throws — note this is fatal to the connection rather than ignored, which is a spec-compliance
problem for the message types listed under [Protocol coverage](#protocol-coverage).

**Receive loop.** `initConnection()` opens the session and runs `receiveAll`, a loop that reads
frames until the channel closes. For each message it calls `handleIncoming`, which is the entire
protocol state machine:

| Incoming | Layer | Effect |
|---|---|---|
| `Open` | SockJS | Sends DDP `connect` with the negotiated version |
| `Close` | SockJS | Closes the connection, then re-raises so the retry machinery reconnects |
| `Heartbeat` | SockJS | Ignored |
| `Message` | SockJS | Ignored |
| `Connected` | DDP | Stores `sessionId`, sets state `DDPConnected`, resets the retry budget, starts keepalive, refreshes the token, and re-subscribes (reconnects only) |
| `Failed` | DDP | Server rejected the version; adopts the server's version if supported, otherwise throws |
| `Ping` | DDP | Replies `pong`, echoing the `id` if present |
| `Pong` | DDP | Resolves the matching pong listener |
| `Added` / `Changed` / `Removed` | DDP | Applied to the `Database` |
| `Ready` | DDP | Resolves the matching subscription listeners |
| `NoSub` | DDP | Subscription failed or was cancelled; routes to the subscription's error/retry path |
| `Result` | DDP | Resolves the matching method listener |
| `Updated` | DDP | Signals that the method's writes are visible; emits `MethodState.Updated` |
| `Error` | DDP | Ignored |
| `Exception` | internal | Not a wire message — see below |

### Protocol coverage

`Incoming` mixes three layers, which is worth knowing before you `when`-match on it. Measured
against the [DDP specification](https://github.com/meteor/meteor/blob/devel/packages/ddp/DDP.md):

**Genuine DDP messages** — `Connected`, `Failed`, `Ping`, `Pong`, `Added`, `Changed`, `Removed`,
`Ready`, `NoSub`, `Result`, `Updated`, `Error`. All spec-defined, with spec field names.

**SockJS transport frames, not DDP** — `Open`, `Close`, `Heartbeat`, `Message`. These come from the
SockJS envelope (`o`, `c`, `h`, `m` prefixes), not from DDP, and their `msg` strings (`"open"`,
`"close"`, …) are this library's invention. A DDP implementation over a raw websocket would not have
them.

**Not a message at all** — `Incoming.Exception(exception: Throwable)`. This is a client-internal
signal for transport failures, injected by the connection flow so callers see errors on the same
stream. It is the only `Incoming` subtype that is not `@Serializable`, it never appears on the wire,
and its `msg` value `"exception"` is not a DDP message type. Do not treat it as one.

**Spec messages this client does not implement** — `addedBefore` and `movedBefore`, the ordered-
collection messages a server sends when a publication uses an ordered observe. The converter throws
`IllegalStateException` on any unrecognised `msg`, and because that type is not in the handled-
exception set the connection degrades straight to `Disconnected` without retrying. If your server
publishes ordered collections, this client cannot talk to it. See
[Known limitations](#known-limitations).

`randomSeed` is implemented and matches Meteor's algorithm — see [Predictable ids](#predictable-ids-randomseed).

Every message is also republished on `client.ddpMessages`, a `SharedFlow<Incoming>` you can observe
for logging or diagnostics. It is buffered (64) and drops oldest under pressure, so a slow collector
there can never stall the socket.

**Correlation.** DDP is request/response over a single socket, so replies are matched by id through
three maps: `methodResultListeners`, `subscriptionResultListeners`, `pongListeners`. Each `call()` /
`subscribe()` registers a listener on start and removes it in `awaitClose`.

## Configuration reference

All settings are configured in the `DDPClient` trailing lambda, and most are also mutable on the
client afterwards.

```kotlin
val client = DDPClient(host = "example.meteor.app") { /* DDPClientConfig */ }
```

| Option | Type | Default | What it does |
|---|---|---|---|
| `protocol` | `URLProtocol` | `WSS` | `WSS` or `WS`. The port is always the protocol's default port. |
| `socketPath` | `String?` | random `sockjs/…/websocket` | Path appended to the host. Override for non-standard SockJS mounts. |
| `ddpVersion` | `String` | `"1"` | Version offered in `connect`. Supported: `1`, `pre1`, `pre2`. If the server replies `failed` with another supported version, the client adopts it and retries. |
| `json` | `Json` | lenient, `ignoreUnknownKeys`, `explicitNulls = false`, `coerceInputValues` | Used for every encode/decode, including your method results and collection documents. |
| `db` | `Database` | `InMemoryDatabase(json)` | The local minimongo mirror. See [Implementing your own Database](#implementing-your-own-database). |
| `httpClientConfig` | `HttpClientConfig<*>.() -> Unit` | `{}` | Extra ktor configuration (logging, proxies, custom headers). |
| `retryConnection` | `Boolean` | `true` | Master switch for automatic reconnect. `false` means one attempt, then `Disconnected`. |
| `maxReconnectAttempts` | `Int` | `3` | Reconnect attempts **per outage** — the counter resets on every successful connect, so it is not a lifetime budget. |
| `retryDelay` | `Long` (ms) | `500` | Delay between reconnect attempts. The first retry is immediate; subsequent ones wait. Fixed delay, no backoff. |
| `timeout` | `Long` (ms) | `20_000` | Deadline for a method call to reach a terminal state, and the deadline for waiting on a reconnect during a retry. Deliberately generous: heavy server methods legitimately exceed 10 s, and a shorter deadline reported false timeouts for calls the server had already committed. |
| `pingPongEnabled` | `Boolean` | `true` | Whether to run the keepalive loop. |
| `pingPongInterval` | `Long` (ms) | `30_000` | Gap between pings. |
| `pingPongTimeout` | `Long` (ms) | `15_000` | How long to wait for a pong before declaring the socket dead and restarting. |
| `tokenRefresher` | `suspend () -> Boolean` | `{ true }` | Re-authenticate. See [Authentication](#authentication). |
| `unauthorizedChecker` | `(String, JsonArray?, ResponseError) -> Boolean` | `{ _, _, _ -> false }` | Decides whether an error means "token expired". |

**Supplying your own HTTP client.** The secondary constructor takes a `ClientFactory`
(`((HttpClientConfig<*>) -> Unit) -> HttpClient`), which lets you control engine selection while the
client still installs its own `WebSockets` plugin and converter on top:

```kotlin
val client = DDPClient(
    host = "example.meteor.app",
    clientFactory = { configure -> HttpClient(CIO) { configure(this) } },
) { /* config */ }
```

## Connection lifecycle

`client.connectionState: StateFlow<ConnectionState>`:

| State | Meaning |
|---|---|
| `NotConnected` | Initial, or cleanly closed. |
| `Connecting(exception)` | Attempting to connect. `exception` is the failure that triggered this retry, or `null` for a first attempt. |
| `DDPConnected` | Socket open **and** the DDP `connected` handshake completed. This is the only state in which messages actually go out. |
| `Disconnected(exception)` | The retry budget is spent or the failure was not retryable. Terminal until you call `resumeConnection()` or `reconnect()`. |
| `Paused` | Deliberately suspended by `pauseConnection()`. The retry machinery ignores this state, so a paused client stays down. |

Control methods:

| Method | Behaviour |
|---|---|
| `initConnection(): Flow<Incoming>` | The connection itself. Collect it once to run the client. Never throws — failures become `Incoming.Exception` plus a `Disconnected` state. |
| `restartConnection()` | Tears down and reconnects. **No-op while `Paused`.** |
| `resumeConnection()` | Reconnects from `Paused` or `Disconnected`. No-op otherwise. |
| `reconnect()` | Reconnects from any state — `resumeConnection()` if paused, `restartConnection()` otherwise. Used internally by the method retry paths. |
| `pauseConnection()` | Sets `Paused` and closes the socket. Call when your app backgrounds. |
| `closeConnection(paused = false)` | Closes the socket and fails every in-flight method call with `onConnectionClosed`. |

A typical mobile lifecycle binding:

```kotlin
override fun onStart()  { scope.launch { client.resumeConnection() } }
override fun onStop()   { scope.launch { client.pauseConnection() } }
```

## Reconnect and retry

There are **three independent retry layers**. They compose, which is why a dropped socket mid-call
usually recovers without the caller noticing.

### 1. Connection-level reconnect

Driven by `retryWhen` around the socket flow. On any failure:

1. If `retryConnection` is `false`, stop.
2. If the state is `Paused`, stop — a deliberate pause must not be undone by the retry loop.
3. If the exception is not one of the handled kinds, stop (and log it as a non-fatal).
4. If `connectionRetryCount >= maxReconnectAttempts`, stop → `Disconnected`.
5. Otherwise set `Connecting`, wait `retryDelay` (except on the first retry, which is immediate),
   and reconnect.

Handled exceptions are the ones that mean "the network went away, not the code is wrong":
`ClosedReceiveChannelException`, `ClosedSendChannelException`, `UnresolvedAddressException`,
`ConnectTimeoutException`, `WebSocketException`, `CancellationException`, plus a per-platform set
(`platformExceptions`) covering the Darwin and OkHttp engines' own socket errors.

**The budget is per-outage.** `connectionRetryCount` resets to `0` on every successful
`Incoming.Connected`. `retryWhen`'s own attempt counter is monotonic for the flow's lifetime, which
would treat `maxReconnectAttempts` as a lifetime allowance — after three unrelated blips over a long
session (TLS hiccup at launch, backgrounding, the server closing the socket after a signup) the next
drop would strand the client `Disconnected` forever.

**On a successful reconnect** the client: resets the budget, restarts the keepalive, runs
`tokenRefresher`, and then re-subscribes everything that was `Subscribed` **or still `Subscribing`**
when the socket dropped, reusing the original subscription ids. Restoring in-flight subscriptions
matters — a subscription that had not yet received its `ready` would otherwise be lost for good, and
its screen would never sync. The token refresh and the re-subscribe share one coroutine so they stay
ordered, and both run off the receive loop so a slow refresh cannot block incoming data.

**First connect is not a reconnect.** The restore path is skipped the first time, because nothing has
been lost yet and every subscription in the map was just created by a caller whose own flow is
already sending its `sub`.

### 2. Per-call retry

Every `call()` retries **once**, and only after establishing that the connection — not the server —
was at fault. On an exception or a timeout:

1. If this is already the second attempt, give up: emit `MethodState.Exception` and close.
2. Look at `connectionState`. If it is already `Disconnected` / `Connecting` / `NotConnected` /
   `Paused`, skip diagnosis, call `reconnect()`, wait up to `timeout` for `DDPConnected`, and resend.
   Fail if the reconnect does not land in time.
3. Otherwise the connection *claims* to be healthy, so probe it: send a ping and wait
   `pingPongTimeout`.
   - **Pong arrives** → the socket is fine and the server genuinely failed or was slow. This is a
     real error; report it without retrying.
   - **No pong** → the socket is a zombie. Restart, wait for `DDPConnected`, and resend.

That ping probe is the point of the design: it separates "the server said no" from "the socket died
quietly", and only the second one is worth retrying.

`onConnectionClosed` — a deliberate `closeConnection()` — never retries. It fails all in-flight calls
immediately, because the close was intentional.

### 3. Re-authentication retry

Orthogonal to the other two, and covered in [Authentication](#authentication).

## Keepalive (ping/pong)

While `DDPConnected` and `pingPongEnabled`, a loop sends a `ping` with a fresh id every
`pingPongInterval` and waits `pingPongTimeout` for the matching `pong`. A missing pong calls
`restartConnection()`.

The keepalive is best-effort: if a send throws (typically because a background/foreground race is
tearing the socket down), the loop logs and exits rather than propagating. It runs in a `launch{}`
outside the connection flow's `retryWhen`/`catch`, so an exception there would otherwise reach the
unhandled-coroutine handler and abort the process on Kotlin/Native. Losing the keepalive is
harmless — the receive loop and the reconnect machinery still notice a dead socket.

The client also answers server-initiated pings automatically.

## Authentication

The client has no opinion about authentication. Two lambdas are the entire seam, which is why
nothing app-specific leaks into the library:

| Hook | Signature | Purpose |
|---|---|---|
| `unauthorizedChecker` | `(method: String, params: JsonArray?, error: ResponseError) -> Boolean` | Decide whether a method/subscription error means "token expired". Defaults to `false`, so nothing is ever retried unless you opt in. |
| `tokenRefresher` | `suspend () -> Boolean` | Re-authenticate. Return `true` on success; the failed call is then resent automatically. |

```kotlin
val client = DDPClient(host) {
    unauthorizedChecker = { _, _, error -> error.error == "unauthorized" }
    tokenRefresher = { myAuth.refresh() }
}
client.setAuthenticationState(AuthenticationState.Authorized) // after your own login
```

`setAuthenticationState(Authorized)` matters: `tokenRefresher` only runs when the client already
considers itself authorized, so a client that never logged in will not try to refresh.

### The authentication state machine

`client.authenticationState: StateFlow<AuthenticationState>` is `Unauthorized` → `Authorizing` →
`Authorized`. `Authorizing` doubles as a **lock**:

- `refreshToken()` enters via an atomic compare-and-set from `Authorized` to `Authorizing`. Only the
  winner runs `tokenRefresher`; concurrent callers no-op. Without the CAS, N calls failing at once on
  the same expired token produced N logins.
- Everything waiting on authentication — `sendMessage`, the method and subscription re-auth flows —
  parks while `Authorizing` is set.
- The state is released in a `finally`, and falls back to `Authorized` if the refresh was cancelled.
  A cancelled refresh (socket dropped mid-refresh) learned nothing bad about the token, and the next
  `unauthorized` response will simply try again. Leaving it stuck in `Authorizing` used to hang every
  subsequent send permanently.

### The re-auth retry

When a method result or a `nosub` carries an error and `unauthorizedChecker` returns `true`, the
client calls `refreshToken()` and then waits on `authenticationState`:

- `Authorized` → resend the original message once (guarded by a `resent` flag, so it cannot loop).
- `Unauthorized` → give up and surface the original error to the caller.

The refresher job is cancelled when the caller's flow closes, so an abandoned screen does not keep a
retry alive.

## Method calls

```kotlin
fun <reified T : Any> call(
    method: String,
    params: JsonArray? = null,
    randomSeed: String? = null,
): Flow<MethodState<T>>
```

`T` is deserialized from the DDP `result` field using the configured `json`. Use
`JsonElement` for `T` if you want the raw payload. For `randomSeed`, see below.

`MethodState<T>` is a sealed hierarchy:

| State | When |
|---|---|
| `Loading` | Emitted immediately on subscription. |
| `Success(response: T?)` | The server returned a result. `response` is `null` if the method returned nothing. Terminal. |
| `Updated` | The server confirmed the method's database writes are visible. |
| `Error(error: ResponseError?)` | The server returned an error. Terminal. |
| `Exception(exception: Throwable?)` | Transport failure, or timeout (`TimeoutException`). Terminal. |
| `Idle` | Never emitted by the client — provided for callers modelling a not-yet-started call. |

### Working with MethodState

Rather than exhaustive `when` blocks everywhere, there are predicates and transforms:

```kotlin
// Predicates
state.loading                 // Loading or Updated
state.succeeded               // Success with a non-null response
state.succeededWithoutContent // Success, response may be null
state.failed                  // Error or Exception
state.idle
state.content                 // T? — the response, or null for any non-Success state
state.error                   // MethodState.Error?
state.exception               // MethodState.Exception?
```

```kotlin
// Map the payload, preserving the state envelope
client.call<UserDto>("users.get").transform { dto -> dto?.toUser() }
client.call<UserDto>("users.get").transformNotNull { dto -> dto.toUser() }  // errors if null

// Chain a second call on the result of the first
client.call<Session>("login")
    .flatMapLatestMethodState { session -> client.call<User>("users.me", session.params()) }
```

`transformNotNull` and `flatMapLatestMethodState` throw if `Success` carries a `null` response — use
`transform` when the method can legitimately return nothing.

### Predictable ids (`randomSeed`)

The DDP spec says of `randomSeed`:

> By using the same seed with the same algorithm, the same pseudo-random values can be generated on
> the client and the server. In particular, this is used for generating ids for newly created
> documents.

"The same algorithm" is the operative phrase — the seed is only useful if both sides run *bit-identical*
PRNGs. `io.bordo.ddpclient.random.MeteorRandom` is a 1:1 port of Meteor's `random` package: the
[Alea](http://baagoe.org/en/wiki/Better_random_numbers_for_javascript) generator and its `Mash` seed
hash, including the `>>> 0` (`ToUint32`) truncation at each step, Meteor's 54-character
`UNMISTAKABLE_CHARS` alphabet, and its 17-character default id length. The arithmetic is deliberately
un-simplified — "cleaning it up" would silently desynchronise it from the server.

This lets you do optimistic inserts: predict the `_id` the server will assign *before* the method
round-trips, render the document immediately, and have the server's eventual `added` be a no-op
rather than a duplicate.

```kotlin
val seed = MeteorRandom.newSeed()                                // 20 hex chars, like Meteor
val predictedId = MeteorRandom.predictDocumentId(seed, "messages")

renderOptimistically(Message(_id = predictedId, text = text))    // instant UI

client.call<Unit>("messages.insert", params, randomSeed = seed).collect { … }
```

`predictDocumentId` mirrors Meteor's scoping exactly: the server seeds its generator with
`[randomSeed, "/collection/<name>"]` and draws the id as the first value from that stream, so the
prediction holds as long as the method's first action on that collection is the insert. A method that
draws other random values first, or inserts into several collections, will diverge — the sequence is
positional.

The full generator is public if you need more than ids:

```kotlin
val gen = MeteorRandom.createWithSeeds(seed, "/collection/messages")  // deterministic
gen.id()            // 17 chars of UNMISTAKABLE_CHARS
gen.hexString(20)
gen.secret()        // 43 base64 chars
gen.fraction()

MeteorRandom.insecure()   // non-deterministic, backed by kotlin.random.Random
```

Alea is **not** cryptographically strong — matching Meteor is the point, not unpredictability. Do not
use it for tokens.

## Subscriptions

```kotlin
fun subscribe(name: String, params: JsonArray? = null, subscriptionId: String? = null): SubscriptionFlow
```

`SubscriptionFlow` is a `Flow<Subscription>` that also exposes its `subscription` (id, name, params,
state) up front, so you can read the id without collecting.

```kotlin
// launch and forget; cancel the scope to unsubscribe
client.subscribe("messages", buildJsonArray { add(roomId) }).launch(scope)

// or observe the state
client.subscribe("messages").collect { sub ->
    when (sub.state) {
        SubscriptionState.Subscribing  -> showSpinner()
        SubscriptionState.Subscribed   -> hideSpinner()   // 'ready' received
        is SubscriptionState.Error     -> showError(sub.state.error)
        SubscriptionState.Unsubscribing,
        SubscriptionState.Unsubscribed -> Unit
    }
}
```

To react to each state change without taking over collection, chain `onEachSubscription` — it keeps
the `SubscriptionFlow` type, so `launch` still works afterwards:

```kotlin
client.subscribe("messages")
    .onEachSubscription { sub -> log(sub.state) }
    .launch(scope)
```

Subscribing does **not** hand you the data — it tells the server to start streaming into your
`Database`. Read the data with [collection flows](#collections).

Unsubscribing:

```kotlin
client.unsubscribe(id).collect { }              // one subscription
client.unsubscribeFromAll().collect { }         // all active ones
client.unsubscribeFrom { it.name == "messages" }.collect { }  // by predicate
```

The bulk variants emit `UnsubscribeAllState`: `NotStarted` → `StartedSome` → `StartedAll` →
`UnsubscribedAll`, combined across every subscription involved. They short-circuit to
`UnsubscribedAll` when nothing matches.

Inspection: `client.subscriptions` (all, by id), `client.activeSubscriptions` (state `Subscribed`),
`client.subscriptionCount`. `client.clearSubscriptions()` drops the bookkeeping without sending
`unsub` — the reconnect path uses it; you normally want `unsubscribeFromAll()` instead.

## Collections

Subscription data lands in `client.db`, a minimongo mirror keyed by collection name. Documents are
stored as `JsonObject` with the DDP id injected as `_id`.

### Snapshots

```kotlin
client.db.getCollection<User>("users")      // List<User>? — decoded, null if unknown
client.db.getRawCollection("users")         // JsonArray?  — undecoded
client.db.collections                       // Map<String, DbCollection>
client.db.collectionNames                   // List<String>
client.db.dump()                            // pretty-printed JSON of every collection, for debugging
```

### Live flows

```kotlin
// Decoded, re-emitted on every change
client.db.receiveCollection<User>("users").collect { users -> render(users) }

// Raw, if you want to decode yourself or inspect the wire shape
client.db.receiveRawCollection("users").collect { array -> … }

// Filtered — and debounced (see below)
client.db.receiveCollection<Message>("messages") { it.roomId == roomId }
    .collect { messages -> render(messages) }
```

The underlying `getCollectionFlow(name)` is a `SharedFlow` with `replay = 10`, so a collector
attaching after the data arrived still sees the current contents.

### Burst coalescing and paging

A Meteor page arrives as a **burst of one `added` per document**. The unfiltered
`receiveCollection<T>(name)` re-emits on every one of them, so a 50-document page produces 50
emissions and a list that visibly grows a row at a time.

The **filtered** overload solves this: it debounces by `BURST_SETTLE_MILLIS` (50 ms) and applies
`distinctUntilChanged`, collapsing the burst into a single emission of the finished list. If you are
paging, prefer it — pass `{ true }` as the filter if you do not actually need to filter:

```kotlin
client.db.receiveCollection<Message>("messages") { true }
```

Two caveats:

- **Sort before truncating.** DDP does not guarantee insertion order matches your sort order, so a
  window taken off the raw list keeps whichever documents happened to arrive first, not the newest
  ones. Sort, then take your page:
  ```kotlin
  client.db.receiveCollection<Message>("messages") { it.roomId == roomId }
      .map { it.sortedByDescending(Message::createdAt).take(pageSize) }
  ```
- **Debounce, not sample.** The 50 ms window never fires while updates keep arriving closer together
  than that. Human-paced traffic settles fine; a permanently busy collection would stall.

`BURST_SETTLE_MILLIS` is a public constant but not currently configurable per call.

### Clearing

`client.dropDb()` clears every collection, resets each flow's replay cache, and pushes an empty
snapshot. Call it on logout or tenant switch — without the replay reset, a newly attached collector
would still be served the previous session's documents out of the replay buffer.

## Implementing your own Database

The default `InMemoryDatabase` keeps everything in memory and loses it on process death. To persist
(SQLDelight, Room, Realm) or to add indexing, implement `Database` and pass it as `db`.

```kotlin
val client = DDPClient(host) {
    db = MyPersistentDatabase(json)
}
```

### The contract

`Database` extends `DataStore`, which is the write side the receive loop calls:

```kotlin
interface DataStore {
    suspend fun onDataAdded(collectionName: String, documentID: String, newValues: JsonObject)
    suspend fun onDataChanged(
        collectionName: String,
        documentID: String,
        updatedValues: JsonObject?,   // fields to merge; null if this is a clear-only message
        removedFields: List<String>?, // fields to delete; null if none
    )
    suspend fun onDataRemoved(collectionName: String, documentID: String)
}

interface Database : DataStore {
    val json: Json
    val collections: Map<String, DbCollection>
    fun getCollectionFlow(name: String): SharedFlow<DbCollection>
    fun dump(): String
    fun drop()
}
```

Rules your implementation must honour:

1. **Auto-create collections.** `added` for an unknown collection creates it. Never fail on a name
   you have not seen.
2. **`changed` upserts.** DDP sends `changed` for a document you may not hold (it was added before
   you attached, or evicted). Treat `changed` for an unknown id as an `added`. `InMemoryDatabase`
   does exactly this.
3. **`changed` merges, it does not replace.** Fields in `updatedValues` are merged over the existing
   document; untouched fields survive. `removedFields` deletes keys.
4. **Store the id as `_id`.** Collection flows and `getDocument` look documents up by the `_id` field
   inside the document, not by an external key.
5. **Emit after every mutation.** Push the updated collection onto the flow returned by
   `getCollectionFlow(name)` at the end of each `onData*` call, or nothing observing the collection
   will ever update.
6. **`getCollectionFlow` must be idempotent and must replay.** Return the same flow instance for the
   same name, creating it on first request. Give it a replay buffer so a late collector sees current
   state rather than waiting for the next change.
7. **`drop()` must reset replay caches too**, not just the stored documents — otherwise stale
   documents are re-delivered to new collectors after a logout.
8. **The `onData*` methods are called from the receive loop.** Keep them fast; do not block. If your
   storage is slow, buffer internally and write asynchronously — stalling here stalls all incoming
   data, including method results and `ready` messages.
9. **Assume concurrent reads.** Collectors decode documents on other threads while the receive loop
   writes. See [Concurrency notes](#concurrency-notes).

`DbCollection` is the per-collection interface — `documents: JsonArray`, `documentIds`,
`getDocument`, `putDocument`, `updateDocument`, `removeFields`, `removeDocument`, `removeAll`. The
`receiveCollection` / `getCollection` extensions are defined on `Database` and work with any
implementation for free.

## EJSON

Meteor's extended JSON types, as `typealias`es with serializers attached — annotate nothing, just use
the type:

```kotlin
import io.bordo.ddpclient.ejson.EBinary
import io.bordo.ddpclient.ejson.EDate
import io.bordo.ddpclient.ejson.ERegex

@Serializable
data class Message(
    val _id: String,
    val text: String,
    val createdAt: EDate,   // {"$date": 1735689600000}  <-> kotlin.time.Instant
    val blob: EBinary?,     // {"$binary": "…"}          <-> String
    val pattern: ERegex?,   // {"$regexp": "…", "$flags": "…"} <-> Regex
)
```

`EDate` maps to `kotlin.time.Instant` via epoch milliseconds. `EBinary` maps to the base64 `String`
as-is. `ERegex` maps to `Regex` — note it currently **drops flags on serialization** (it writes an
empty `$flags`), so round-tripping a case-insensitive pattern loses the flag.

## Concurrency notes

Relevant if you are extending the client or writing a `Database`.

**Listener maps are `ConcurrentMutableMap`** (stately). They are read and written from several
coroutines and dispatchers at once — the receive loop, every method/subscription flow's
`awaitClose`, the keepalive. The declared type is the concrete class, not `MutableMap`, on purpose:
only the concrete type exposes `block { }`.

`ConcurrentMutableMap` locks **per operation**, not per traversal. Iterating `values` or `keys` takes
and releases the lock on each `next()`, so a concurrent put or remove mid-traversal still throws
`ConcurrentModificationException` from the backing `LinkedHashMap` — and an exception escaping the
receive loop kills all sync. Any read that walks the whole map, and any read-then-write pair that
must be atomic, has to go inside `block { }`:

```kotlin
// copy and clear as ONE critical section
val listeners = methodResultListeners.block { map -> map.values.toList().also { map.clear() } }
```

**Documents are materialized before publication.** `kotlinx.serialization`'s `JsonObject` builds some
internal structures lazily on first access. On Kotlin/Native, several collectors decoding the same
document concurrently can race that lazy initialization and crash. `InMemoryCollection` calls
`materializeForConcurrentDecode()` on every document at ingestion — single-threaded, on the receive
loop — before it becomes visible. A custom `DbCollection` that stores `JsonObject`s should do the
same.

## Building and testing

```bash
./gradlew build
```

Compiles every target and runs the suite. Tests that need a live websocket server are marked
`@IgnoreNative` — ktor's `testApplication` has no websocket engine on Kotlin/Native — so those run on
the JVM host only; everything else runs on every target.

```bash
./gradlew build --rerun-tasks
```

Use `--rerun-tasks` when comparing runs: Gradle's up-to-date checks will otherwise report
`BUILD SUCCESSFUL` without executing anything.

## Known limitations

### Spec compliance

Verified against the [DDP specification](https://github.com/meteor/meteor/blob/devel/packages/ddp/DDP.md).
All three of these are fixable without changing the public API.

- **`addedBefore` and `movedBefore` are not implemented.** A server publishing an ordered collection
  sends these, the converter throws `IllegalStateException` on the unrecognised `msg`, and since that
  type is not in the handled-exception set the client goes to `Disconnected` *without retrying*. An
  unknown message type should be ignored rather than fatal, independently of whether the ordered
  messages themselves get support.
- **`added` requires `fields`, but the spec makes it optional.** `Incoming.Added.fields` is a
  non-nullable `JsonObject` with no default, so a spec-legal `{"msg":"added","collection":…,"id":…}`
  fails to deserialize with `MissingFieldException` and kills the connection. (`changed` is correct —
  its `fields` is nullable.)
- **`ResponseError` requires `reason`, `message` and `errorType`, but the spec requires only `error`.**
  A minimal server error such as `{"error":"not-found"}` throws `MissingFieldException`, so the error
  never reaches your `unauthorizedChecker` or your `MethodState.Error`. The spec also notes `error`
  was historically a number; a numeric `error` fails here too.

### Defects

- **`ERegex` drops regex flags on serialization** — it always writes an empty `$flags`. Fixable, needs
  a Kotlin `RegexOption` ↔ JavaScript flag mapping.
- **`receiveCollection` can emit an empty snapshot after a non-empty one** in some sequences. If you
  render directly off it, guard against a spurious empty list. Not yet root-caused.

### Scope

- **No JVM target.** Needs `actual`s for the HTTP engine, platform exceptions, and UUID. Non-breaking
  to add.
- **No Swift Package.** `DDPClient.call<T>()` is `inline` + `reified` and cannot be exported to
  Objective-C, so a usable Swift API needs non-reified overloads first.

## License

Apache 2.0 — see [LICENSE](LICENSE).
