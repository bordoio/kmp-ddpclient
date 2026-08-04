package io.bordo.ddpclient.util

import io.ktor.client.engine.darwin.DarwinHttpRequestException
import io.ktor.utils.io.errors.IOException
import kotlin.reflect.KClass

/**
 * Created by Osman Saral on 5.04.2024
 */

actual val platformExceptions: Set<KClass<out Exception>>
    get() = setOf(
        // Parity with Android, which allowlists the whole java.io.IOException family: on native,
        // ktor's io.ktor.utils.io.errors.IOException is the umbrella for every transport failure
        // the Darwin engine can throw. This includes DarwinHttpRequestException (kept below for
        // clarity) and, crucially, SocketTimeoutException: handleNSError maps NSURLErrorTimedOut —
        // the one NSError not wrapped in DarwinHttpRequestException — to it, which is what a
        // network drop while idle produces. Before it was allowlisted, that exception bypassed
        // the retry/degrade path and aborted the app as an unhandled coroutine exception.
        IOException::class,
        DarwinHttpRequestException::class,
    )