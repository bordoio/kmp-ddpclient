package io.bordo.ddpclient.ejson

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue

/**
 * EJSON `$regexp`. Untested until now.
 */
class ERegexTests {

    @Serializable
    data class Sample(val r: ERegex)

    @Test
    fun `serializes to a dollar-regexp object with empty flags`() {
        val json = Json.encodeToString(Sample(Regex("a.c")))
        assertEquals("""{"r":{"${'$'}regexp":"a.c","${'$'}flags":""}}""", json)
    }

    @Test
    fun `deserializes the pattern`() {
        val decoded = Json.decodeFromString<Sample>("""{"r":{"${'$'}regexp":"a.c","${'$'}flags":""}}""")

        assertEquals("a.c", decoded.r.pattern)
        assertTrue(decoded.r.matches("abc"))
    }

    @Test
    fun `round-trips the pattern`() {
        // Regex has no structural equals, so the pattern string is the thing to compare.
        val original = Sample(Regex("""^\d{3}-\w+$"""))
        val decoded = Json.decodeFromString<Sample>(Json.encodeToString(original))

        assertEquals(original.r.pattern, decoded.r.pattern)
    }

    @Test
    fun `flags are not carried across`() {
        // Known limitation, flagged by the TODO in ERegexSerializer: options are dropped on
        // serialize and ignored on deserialize. Pinned so a future flag mapping is a deliberate
        // change rather than a surprise.
        val original = Sample(Regex("abc", RegexOption.IGNORE_CASE))
        val json = Json.encodeToString(original)
        assertTrue(json.contains(""""${'$'}flags":""""))

        val decoded = Json.decodeFromString<Sample>(json)
        assertTrue(decoded.r.matches("abc"))
        assertTrue(decoded.r.matches("ABC").not(), "IGNORE_CASE unexpectedly survived the round-trip")
    }

    @Test
    fun `rejects an object missing the flags key`() {
        assertFails { Json.decodeFromString<Sample>("""{"r":{"${'$'}regexp":"a.c"}}""") }
    }

    @Test
    fun `rejects a bare string`() {
        assertFails { Json.decodeFromString<Sample>("""{"r":"a.c"}""") }
    }
}
