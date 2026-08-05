# Changelog

## Unreleased

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
