package io.bordo.ddpclient.messageparser

import io.bordo.ddpclient.ddpclient.defaultJson
import io.bordo.ddpclient.util.clearMessage
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `clearMessage` unwraps a SockJS frame -- `a["<json string>"]` -- back into its JSON payload,
 * undoing the string escaping SockJS applies.
 *
 * This was previously a dump of ~200 frames captured from a live server, asserting only that each
 * one parsed. That corpus carried real customer and employee data (email addresses, chat message
 * bodies, internal hostnames) and was dropped when the library was open sourced. The cases below
 * are synthetic and cover the same escaping shapes the corpus contained -- escaped quotes, doubled
 * backslashes, non-ASCII and emoji, JSON nested inside a string value, large payloads -- plus the
 * branches of `clearMessage` itself, which the corpus never targeted.
 */
class ClearMessageTests {

    private val json = Json(defaultJson) { }

    private fun assertUnwrapsToJson(frame: String) {
        val unwrapped = clearMessage(frame)
        try {
            json.parseToJsonElement(unwrapped).jsonObject
        } catch (e: Exception) {
            throw AssertionError("Did not unwrap to a JSON object.\nFrame: $frame\nGot: $unwrapped", e)
        }
    }

    @Test
    fun `unwraps ordinary ddp frames`() {
        listOf(
            $$"""a["{\"msg\":\"connected\",\"session\":\"bu7rndmJPy7e59bti\"}"]""",
            $$"""a["{\"msg\":\"ready\",\"subs\":[\"9cd2dc69-d830-4147-a384-a75d3789ae6c\"]}"]""",
            $$"""a["{\"msg\":\"removed\",\"collection\":\"messages\",\"id\":\"hAaHDh9dhqzGoWYiz\"}"]""",
            $$"""a["{\"msg\":\"added\",\"collection\":\"counts\",\"id\":\"opened\",\"fields\":{\"count\":57}}"]""",
            $$"""a["{\"msg\":\"result\",\"id\":\"707c42eb-b46f-447f-87e0-4f10bbec59ff\",\"result\":null}"]""",
        ).forEach(::assertUnwrapsToJson)
    }

    @Test
    fun `unwraps escaped quotes inside string values`() {
        assertUnwrapsToJson(
            $$"""a["{\"msg\":\"added\",\"collection\":\"messages\",\"id\":\"a1\",\"fields\":{\"text\":\"she said \\\"hello\\\" twice\"}}"]"""
        )
    }

    @Test
    fun `unwraps doubled backslashes`() {
        assertUnwrapsToJson(
            $$"""a["{\"msg\":\"added\",\"collection\":\"files\",\"id\":\"a2\",\"fields\":{\"path\":\"C:\\\\Users\\\\tmp\"}}"]"""
        )
    }

    @Test
    fun `unwraps non-ascii text and emoji`() {
        assertUnwrapsToJson(
            $$"""a["{\"msg\":\"added\",\"collection\":\"messages\",\"id\":\"a3\",\"fields\":{\"text\":\"günaydın 🎉 çğıöşü\"}}"]"""
        )
    }

    @Test
    fun `unwraps json nested inside a string value`() {
        // The worst shape in the original corpus: a field holding an encoded JSON document, so
        // every quote inside is escaped twice.
        assertUnwrapsToJson(
            $$"""a["{\"msg\":\"added\",\"collection\":\"payloads\",\"id\":\"a4\",\"fields\":{\"raw\":\"{\\\"type\\\":\\\"text\\\",\\\"body\\\":\\\"hi\\\"}\"}}"]"""
        )
    }

    @Test
    fun `unwraps urls with slashes`() {
        assertUnwrapsToJson(
            $$"""a["{\"msg\":\"added\",\"collection\":\"media\",\"id\":\"a5\",\"fields\":{\"url\":\"https://example.com/a/b.jpg?x=1&y=2\"}}"]"""
        )
    }

    @Test
    fun `unwraps a large payload`() {
        // The original corpus had single frames over 180 KB.
        val body = "lorem ipsum ".repeat(16_000)
        assertUnwrapsToJson(
            $$"""a["{\"msg\":\"added\",\"collection\":\"messages\",\"id\":\"a6\",\"fields\":{\"text\":\"$$body\"}}"]"""
        )
    }

    // ---- clearMessage's own branches, which the captured corpus never targeted ----------------

    @Test
    fun `unwraps the unquoted array form`() {
        // a[...] without an inner string: the payload is already raw JSON.
        assertEquals("""{"msg":"ping"}""", clearMessage("""a[{"msg":"ping"}]"""))
    }

    @Test
    fun `leaves non-frames untouched`() {
        // Control frames and anything that is not an `a[` frame pass straight through; the
        // converter handles those separately.
        listOf("", "o", "h", """c[1000,"Normal closure"]""", """{"msg":"ping"}""").forEach {
            assertEquals(it, clearMessage(it))
        }
    }

    @Test
    fun `handles an empty payload`() {
        assertEquals("", clearMessage("""a[""]"""))
    }

    @Test
    fun `unescaping only consumes escaped quotes and backslashes`() {
        // \n and \t must survive for the JSON parser to interpret.
        val unwrapped = clearMessage($$"""a["{\"t\":\"line1\nline2\ttabbed\"}"]""")
        assertTrue(unwrapped.contains("""\n"""), "escaped newline was consumed: $unwrapped")
        assertTrue(unwrapped.contains("""\t"""), "escaped tab was consumed: $unwrapped")
        json.parseToJsonElement(unwrapped).jsonObject
    }
}
