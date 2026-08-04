# Changelog

## Unreleased

### 0.1.0

First public release. Extracted from the MonoChatMobile app, where it had been in production use.

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
