# Changelog

## 0.2.0 — 2026-08-05

- **Added a JVM target.** Targets are now Android, JVM, iosArm64, iosSimulatorArm64 and iosX64,
  published as `ddpclient-jvm` and `ddpclient-ejson-jvm`. No API changes — `commonMain` had no
  platform dependencies, so the target needed only the three existing `actual`s.

  The JVM uses the OkHttp engine, the same as Android, and shares all three `actual`s with it from a
  new `jvmSharedMain` source set. That means the `java.io.IOException` allowlist that classifies
  transport failures as recoverable — proven on Android against this protocol — applies to the JVM
  unchanged.

  The full common test suite now runs on the JVM as well as the Android host: 166 tests, including
  the ones that need a live websocket server.

## 0.1.2 — 2026-08-05

Two APIs that never worked. Both were covered by passing tests that asserted the broken behaviour,
which is how they survived to a public release; those tests have been rewritten.

- `SubscriptionFlow.onEachSubscription` never ran its action. It was `also { onEach(action) }` —
  `onEach` is a cold operator that builds a new flow and does nothing until collected, so the
  decorated flow was discarded and the receiver returned unchanged. It now returns the decorated
  flow, re-wrapped so it keeps its `subscription` and stays chainable with `launch`. The returned
  flow is no longer the same instance, which a correct `onEach` cannot avoid.
- `Database.dump()` threw `SerializationException` on `InMemoryDatabase` for every caller. It
  encoded `Map<String, DbCollection>`, and `DbCollection` is an interface with no registered
  polymorphic subclasses. It now serializes the documents as the `JsonObject` tree they already are,
  which also means custom `DbCollection` implementations no longer need to be `@Serializable`.

Neither API is used by the app this library was extracted from, so nothing else changes.

## 0.1.1 — 2026-08-05

- `sendMessage` sent the same message more than once. Both of its branches collected a `StateFlow`
  (`authenticationState` and `connectionState`), neither of which ever completes, so the collector
  stayed subscribed after the message went out and fired again on the next `Authorized` /
  `DDPConnected` — re-sending and emitting a second `MessageState.Success`. On a flaky connection
  that meant duplicate method calls reaching the server. The returned flow now delivers one result
  and completes.

## 0.1.0 — 2026-08-05

First public release, on Maven Central as `io.bordo:ddpclient` and `io.bordo:ddpclient-ejson`.
Extracted from a production app where it had been in use for two years.

Targets: Android, iosArm64, iosSimulatorArm64, iosX64.

Fixes made while preparing the extraction, all previously shipping:

- `refreshToken()` left the client stuck in `AuthenticationState.Authorizing` forever if it was
  cancelled mid-refresh (a socket drop during a token refresh), after which every `sendMessage` on
  a non-connected socket hung.
- Concurrent `unauthorized` responses each ran `tokenRefresher`, causing N logins per expiry.
- `Incoming.Close` was never emitted — the connection ended before collectors could observe it.
- `closeConnection()` and the subscription restore path iterated `ConcurrentMutableMap` views
  without holding the lock, throwing `ConcurrentModificationException` out of the receive loop and
  stopping all sync.
- Subscriptions created immediately after connecting lost their `ready` delivery, because the
  reconnect restore ran on the first connect too and re-registered listeners under the same ids.

Known limitations:

- No JVM target yet (needs actuals for the HTTP engine, platform exceptions, and UUID).
- No Swift Package. `DDPClient.call<T>()` is `inline`+`reified` and cannot be exported to
  Objective-C, so a usable Swift API needs non-reified overloads first.
- `ERegex` drops regex flags on serialization.
