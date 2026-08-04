package io.bordo.ddpclient

import io.bordo.ddpclient.ddpclient.DDPMessageConverter
import io.bordo.ddpclient.ddpclient.Incoming
import io.bordo.ddpclient.ddpclient.Outgoing
import io.bordo.ddpclient.ddpclient.ResponseError
import io.bordo.ddpclient.ddpclient.defaultJson
import io.ktor.http.HttpStatusCode
import io.ktor.util.reflect.typeInfo
import io.ktor.utils.io.charsets.Charsets
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The SockJS/DDP wire contract. Previously only exercised incidentally through the live-server
 * tests, which means it was untested on Kotlin/Native entirely (those tests are `@IgnoreNative`).
 * This runs everywhere and needs no server.
 */
class DDPMessageSerializationTest {

    private val converter = DDPMessageConverter(defaultJson)

    private suspend fun serialize(value: Outgoing): String {
        val frame = converter.serialize(Charsets.UTF_8, typeInfo<Outgoing>(), value)
        assertIs<Frame.Text>(frame)
        return frame.readText()
    }

    private suspend fun deserialize(raw: String): Incoming =
        converter.deserialize(Charsets.UTF_8, typeInfo<Incoming>(), Frame.Text(raw)) as Incoming

    /** SockJS wraps each DDP payload as a one-element JSON array of a JSON *string*. */
    private fun framed(payload: String) = defaultJson.encodeToString(listOf(payload)).let { "a$it" }

    // ---- Outgoing --------------------------------------------------------------------------

    @Test
    fun `every outgoing message serializes with its DDP msg discriminator`() = runTest {
        val cases = listOf(
            Outgoing.Connect(session = null, version = "1", support = listOf("1")) to "connect",
            Outgoing.Ping("p1") to "ping",
            Outgoing.Pong("p1") to "pong",
            Outgoing.Subscribe("s1", "users", null) to "sub",
            Outgoing.Unsubscribe("s1") to "unsub",
            Outgoing.Method("login", null, "m1", null) to "method",
        )

        for ((message, expectedMsg) in cases) {
            val wire = serialize(message)
            // The frame is a JSON array holding one JSON-encoded string, per SockJS.
            assertTrue(wire.startsWith("[\""), "not a SockJS array frame: $wire")
            assertTrue(wire.contains("\\\"msg\\\":\\\"$expectedMsg\\\""), "missing msg=$expectedMsg in $wire")
        }
    }

    @Test
    fun `connect carries session only when resuming`() = runTest {
        assertTrue(serialize(Outgoing.Connect(null, "1", listOf("1"))).contains("session").not())
        assertTrue(serialize(Outgoing.Connect("abc", "1", listOf("1"))).contains("\\\"session\\\":\\\"abc\\\""))
    }

    @Test
    fun `method params and randomSeed survive serialization`() = runTest {
        val params = buildJsonArray { add(buildJsonObject { put("foo", JsonPrimitive("bar")) }) }
        val wire = serialize(Outgoing.Method("m", params, "id1", "seed1"))

        assertTrue(wire.contains("foo"))
        assertTrue(wire.contains("bar"))
        assertTrue(wire.contains("seed1"))
    }

    @Test
    fun `serializing a non-Outgoing value yields an empty frame`() = runTest {
        val frame = converter.serialize(Charsets.UTF_8, typeInfo<String>(), "not a message")
        assertIs<Frame.Text>(frame)
        assertEquals("", frame.readText())
    }

    // ---- Incoming: SockJS control frames ---------------------------------------------------

    @Test
    fun `sockjs control frames map to their objects`() = runTest {
        assertEquals(Incoming.Open, deserialize("o"))
        assertEquals(Incoming.Close, deserialize("""c[1000,"Normal closure"]"""))
        assertEquals(Incoming.Heartbeat, deserialize("h"))
        assertEquals(Incoming.Message, deserialize("m"))
    }

    // ---- Incoming: DDP messages ------------------------------------------------------------

    @Test
    fun `connected failed ping and pong deserialize`() = runTest {
        assertEquals(Incoming.Connected("s1"), deserialize(framed("""{"msg":"connected","session":"s1"}""")))
        assertEquals(Incoming.Failed("pre2"), deserialize(framed("""{"msg":"failed","version":"pre2"}""")))
        assertEquals(Incoming.Ping("p"), deserialize(framed("""{"msg":"ping","id":"p"}""")))
        assertEquals(Incoming.Pong("p"), deserialize(framed("""{"msg":"pong","id":"p"}""")))
    }

    @Test
    fun `added changed and removed deserialize with their payloads`() = runTest {
        val added = deserialize(framed("""{"msg":"added","collection":"users","id":"1","fields":{"name":"a"}}"""))
        assertIs<Incoming.Added>(added)
        assertEquals("users", added.collection)
        assertEquals("a", added.fields["name"]?.jsonPrimitive?.content)

        val changed = deserialize(
            framed("""{"msg":"changed","collection":"users","id":"1","fields":{"name":"b"},"cleared":["surname"]}""")
        )
        assertIs<Incoming.Changed>(changed)
        assertEquals(listOf("surname"), changed.cleared)

        val removed = deserialize(framed("""{"msg":"removed","collection":"users","id":"1"}"""))
        assertIs<Incoming.Removed>(removed)
        assertEquals("1", removed.id)
    }

    @Test
    fun `changed tolerates absent fields and cleared`() = runTest {
        // The server sends only what actually changed, so both are nullable on the wire.
        val changed = deserialize(framed("""{"msg":"changed","collection":"users","id":"1"}"""))
        assertIs<Incoming.Changed>(changed)
        assertNull(changed.fields)
        assertNull(changed.cleared)
    }

    @Test
    fun `ready nosub result and updated deserialize`() = runTest {
        val ready = deserialize(framed("""{"msg":"ready","subs":["s1","s2"]}"""))
        assertIs<Incoming.Ready>(ready)
        assertEquals(listOf("s1", "s2"), ready.subs)

        val noSub = deserialize(framed("""{"msg":"nosub","id":"s1"}"""))
        assertIs<Incoming.NoSub>(noSub)
        assertNull(noSub.error)

        val result = deserialize(framed("""{"msg":"result","id":"m1","result":{"ok":true}}"""))
        assertIs<Incoming.Result>(result)
        assertNull(result.error)

        val updated = deserialize(framed("""{"msg":"updated","methods":["m1"]}"""))
        assertIs<Incoming.Updated>(updated)
        assertEquals(listOf("m1"), updated.methods)
    }

    @Test
    fun `result error deserializes into every ResponseError field`() = runTest {
        val raw = framed(
            """{"msg":"result","id":"m1","error":{"error":"403","reason":"nope","message":"denied",""" +
                """"errorType":"Meteor.Error","details":{"why":"x"}}}"""
        )
        val result = deserialize(raw)
        assertIs<Incoming.Result>(result)

        val error = result.error
        assertEquals("403", error?.error)
        assertEquals("nope", error?.reason)
        assertEquals("denied", error?.message)
        assertEquals("Meteor.Error", error?.errorType)
        assertEquals("x", error?.details?.jsonObjectOrNull()?.get("why")?.jsonPrimitive?.content)
    }

    @Test
    fun `error message deserializes with an offending message`() = runTest {
        val error = deserialize(
            framed("""{"msg":"error","reason":"Bad request","offendingMessage":{"msg":"sub"}}""")
        )
        assertIs<Incoming.Error>(error)
        assertEquals("Bad request", error.reason)
        assertEquals("sub", error.offendingMessage?.get("msg")?.jsonPrimitive?.content)
    }

    @Test
    fun `unknown fields are ignored so added server fields do not break the client`() = runTest {
        // defaultJson sets ignoreUnknownKeys; this is what keeps a server-side field addition from
        // taking the client down, so it is worth pinning.
        val connected = deserialize(framed("""{"msg":"connected","session":"s1","serverTime":123}"""))
        assertEquals(Incoming.Connected("s1"), connected)
    }

    // ---- Incoming: malformed input ----------------------------------------------------------

    @Test
    fun `an unknown msg type is rejected rather than silently dropped`() = runTest {
        assertFailsWith<IllegalStateException> { deserialize(framed("""{"msg":"teleport"}""")) }
    }

    @Test
    fun `a frame that is neither a control frame nor a data frame is rejected`() = runTest {
        assertFailsWith<IllegalStateException> { deserialize("""{"msg":"connected"}""") }
    }

    @Test
    fun `malformed json inside a data frame is rejected`() = runTest {
        assertFailsWith<Exception> { deserialize("""a["{not json}"]""") }
    }

    @Test
    fun `non-text frames and wrong target types are rejected`() = runTest {
        assertFailsWith<IllegalStateException> {
            converter.deserialize(Charsets.UTF_8, typeInfo<Incoming>(), Frame.Binary(true, ByteArray(1)))
        }
        assertFailsWith<IllegalArgumentException> {
            converter.deserialize(Charsets.UTF_8, typeInfo<String>(), Frame.Text("o"))
        }
    }

    @Test
    fun `isApplicable accepts text frames only`() {
        assertTrue(converter.isApplicable(Frame.Text("o")))
        assertTrue(converter.isApplicable(Frame.Binary(true, ByteArray(1))).not())
    }

    // ---- ResponseError helpers ---------------------------------------------------------------

    @Test
    fun `fromHttpError maps status code into the error shape`() {
        val error = ResponseError.fromHttpError(HttpStatusCode.NotFound)
        assertEquals(HttpStatusCode.NotFound.toString(), error.error)
        assertEquals(HttpStatusCode.NotFound.description, error.message)
        assertEquals("http_error", error.errorType)
    }

    private fun kotlinx.serialization.json.JsonElement.jsonObjectOrNull() =
        this as? kotlinx.serialization.json.JsonObject
}
