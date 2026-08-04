package io.bordo.ddpclient

import app.cash.turbine.test
import io.bordo.ddpclient.ddpclient.Incoming
import io.bordo.ddpclient.ddpclient.MethodState
import io.bordo.ddpclient.ddpclient.ResponseError
import io.bordo.ddpclient.ddpclient.content
import io.bordo.ddpclient.ddpclient.error
import io.bordo.ddpclient.ddpclient.exception
import io.bordo.ddpclient.ddpclient.failed
import io.bordo.ddpclient.ddpclient.flatMapLatestMethodState
import io.bordo.ddpclient.ddpclient.idle
import io.bordo.ddpclient.ddpclient.loading
import io.bordo.ddpclient.ddpclient.succeeded
import io.bordo.ddpclient.ddpclient.succeededWithoutContent
import io.bordo.ddpclient.ddpclient.toMethodStateException
import io.bordo.ddpclient.ddpclient.transform
import io.bordo.ddpclient.ddpclient.transformNotNull
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * `MethodFlow`'s operators and predicates are the most-used part of the public API downstream
 * (`content` and `transformNotNull` alone have ~85 call sites in the app) and had no direct tests.
 */
class MethodFlowTest {

    private val error = ResponseError("404", "reason", "message", "type")
    private val boom = IllegalStateException("boom")

    // Explicitly typed: `content` is `MethodState<T>.content: T?`, so on a bare
    // `MethodState<Nothing>` the compiler cannot infer T.
    private val nonSuccessStates: List<MethodState<String>> = listOf(
        MethodState.Error(error),
        MethodState.Exception(boom),
        MethodState.Idle,
        MethodState.Loading,
        MethodState.Updated,
    )

    // ---- transform -------------------------------------------------------------------------

    @Test
    fun `transform maps the success payload`() {
        val result = MethodState.Success("42").transform { it?.toInt() }
        assertEquals(MethodState.Success(42), result)
    }

    @Test
    fun `transform passes null content through to the transformer`() {
        // The distinguishing feature vs transformNotNull: a Success(null) stays a Success.
        val result = MethodState.Success<String>(null).transform { it?.length ?: -1 }
        assertEquals(MethodState.Success(-1), result)
    }

    @Test
    fun `transform leaves every non-success state untouched`() {
        for (state in nonSuccessStates) {
            assertEquals(state, state.transform { error("transformer must not run for $state") })
        }
    }

    @Test
    fun `transformNotNull maps the success payload`() {
        assertEquals(MethodState.Success(3), MethodState.Success("abc").transformNotNull { it.length })
    }

    @Test
    fun `transformNotNull throws on null content`() {
        assertFailsWith<IllegalStateException> {
            MethodState.Success<String>(null).transformNotNull { it.length }
        }
    }

    @Test
    fun `transformNotNull leaves every non-success state untouched`() {
        for (state in nonSuccessStates) {
            assertEquals(state, state.transformNotNull { error("transformer must not run for $state") })
        }
    }

    @Test
    fun `flow transform operators map each emission`() = runTest {
        flowOf<MethodState<String>>(MethodState.Loading, MethodState.Success("7"))
            .transform { it?.toInt() }
            .test {
                assertEquals(MethodState.Loading, awaitItem())
                assertEquals(MethodState.Success(7), awaitItem())
                awaitComplete()
            }

        flowOf<MethodState<String>>(MethodState.Success("abcd"))
            .transformNotNull { it.length }
            .test {
                assertEquals(MethodState.Success(4), awaitItem())
                awaitComplete()
            }
    }

    // ---- flatMapLatestMethodState ----------------------------------------------------------

    @Test
    fun `flatMapLatestMethodState chains the inner flow on success`() = runTest {
        flowOf<MethodState<String>>(MethodState.Success("x"))
            .flatMapLatestMethodState { flowOf(MethodState.Success(it + "y")) }
            .test {
                assertEquals(MethodState.Success("xy"), awaitItem())
                awaitComplete()
            }
    }

    @Test
    fun `flatMapLatestMethodState short-circuits failures without running the transform`() = runTest {
        flowOf<MethodState<String>>(MethodState.Error(error), MethodState.Exception(boom))
            .flatMapLatestMethodState<String, Int> { error("must not run") }
            .test {
                assertEquals(MethodState.Error(error), awaitItem())
                assertEquals(MethodState.Exception(boom), awaitItem())
                awaitComplete()
            }
    }

    @Test
    fun `flatMapLatestMethodState throws on null content`() = runTest {
        assertFailsWith<IllegalStateException> {
            flowOf(MethodState.Success<String>(null))
                .flatMapLatestMethodState { flowOf(MethodState.Success(it)) }
                .test { awaitError().let { throw it } }
        }
    }

    // ---- predicates ------------------------------------------------------------------------

    @Test
    fun `succeeded requires content but succeededWithoutContent does not`() {
        val withContent: MethodState<String> = MethodState.Success("a")
        val withoutContent: MethodState<String> = MethodState.Success(null)

        assertTrue(withContent.succeeded)
        assertTrue(withContent.succeededWithoutContent)

        // This pair is the reason both exist: a method that legitimately returns nothing still
        // succeeded. Confusing them turns an empty-but-fine response into a failure.
        assertFalse(withoutContent.succeeded)
        assertTrue(withoutContent.succeededWithoutContent)
    }

    @Test
    fun `failed covers Error and Exception only`() {
        assertTrue(MethodState.Error(error).failed)
        assertTrue(MethodState.Exception(boom).failed)
        assertFalse(MethodState.Loading.failed)
        assertFalse(MethodState.Idle.failed)
        assertFalse(MethodState.Updated.failed)
        assertFalse(MethodState.Success("a").failed)
    }

    @Test
    fun `loading covers Updated as well as Loading`() {
        // Updated means the server applied the write but the result is still outstanding, so from
        // the caller's point of view the call is still in flight.
        assertTrue(MethodState.Loading.loading)
        assertTrue(MethodState.Updated.loading)
        assertFalse(MethodState.Idle.loading)
        assertFalse(MethodState.Success("a").loading)
    }

    @Test
    fun `idle error and exception accessors`() {
        assertTrue(MethodState.Idle.idle)
        assertFalse(MethodState.Loading.idle)

        // Typed as MethodState<*> so the `error`/`exception` *extensions* resolve: on a
        // MethodState.Error receiver the data-class property of the same name shadows them.
        val errorState: MethodState<*> = MethodState.Error(error)
        val exceptionState: MethodState<*> = MethodState.Exception(boom)
        val loadingState: MethodState<*> = MethodState.Loading

        assertEquals(error, errorState.error?.error)
        assertNull(loadingState.error)

        assertSame(boom, exceptionState.exception?.exception)
        assertNull(loadingState.exception)
    }

    @Test
    fun `content is null for every non-success state`() {
        assertEquals("a", MethodState.Success("a").content)
        for (state in nonSuccessStates) {
            assertNull(state.content)
        }
    }

    // ---- Incoming to MethodState -----------------------------------------------------------

    @Test
    fun `incoming error and exception convert to MethodState Exception`() {
        val fromError = Incoming.Error("why", null).toMethodStateException()
        assertIs<MethodState.Exception>(fromError)
        assertEquals("why", fromError.exception?.message)

        val fromException = Incoming.Exception(boom).toMethodStateException()
        assertSame(boom, fromException.exception)
    }
}
