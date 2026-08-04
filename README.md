# ddpclient

A Kotlin Multiplatform client for [Meteor](https://www.meteor.com/)'s DDP protocol over SockJS
websockets: connection management with reconnect, subscriptions backed by an in-memory minimongo
mirror, and method calls exposed as coroutine `Flow`s.

Targets: **Android**, **iosArm64**, **iosSimulatorArm64**, **iosX64**.

## Install

```kotlin
// build.gradle.kts
kotlin {
    sourceSets {
        commonMain.dependencies {
            implementation("io.bordo:ddpclient:0.1.0")
        }
    }
}
```

`io.bordo:ddpclient-ejson` is published separately for the EJSON surrogates (`$date`, `$binary`,
`$regexp`) if you need them without the client. The client already exposes it as an `api`
dependency.

## Quickstart

```kotlin
val client = DDPClient("example.meteor.app") {
    // every setting below is optional; these are the defaults
    protocol = URLProtocol.WSS
    retryConnection = true
    maxReconnectAttempts = 3
    timeout = 20_000
}

// Collect the connection to start it. It never throws: every failure arrives as
// Incoming.Exception and the client moves to ConnectionState.Disconnected.
scope.launch { client.initConnection().collect() }

// Methods are Flows of MethodState
client.call<User>("users.get", buildJsonArray { add("someId") })
    .collect { state ->
        when {
            state.loading -> showSpinner()
            state.succeeded -> render(state.content!!)
            state.failed -> showError(state.error)
        }
    }

// Subscriptions feed the local database; collect a collection to observe it
client.subscribe("users")
client.db.receiveCollection<User>("users").collect { users -> render(users) }
```

## Bring your own auth

The client has no opinion about authentication. Two config lambdas are the entire seam, which is
why nothing app-specific leaks into the library:

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

Other injection points: `db` (swap the `Database` implementation), `json` (your own
`kotlinx.serialization` `Json`), `httpClientConfig`, and a `clientFactory` for supplying your own
ktor `HttpClient`.

## Connection lifecycle

`connectionState: StateFlow<ConnectionState>` reports `NotConnected` / `Connecting` /
`DDPConnected` / `Disconnected` / `Paused`. Call `pauseConnection()` when your app backgrounds and
`resumeConnection()` when it returns; `resumeConnection()` also recovers from `Disconnected` after
the reconnect budget has been spent.

## Building

```bash
./gradlew build
```

Runs all targets and the test suite. The tests that need a live websocket server are marked
`@IgnoreNative` — ktor's `testApplication` has no websocket engine on Kotlin/Native, so those run
on the JVM host only; everything else runs on every target.

## License

Apache 2.0 — see [LICENSE](LICENSE).
