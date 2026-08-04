package io.bordo.ddpclient.ejson

import kotlin.time.Instant
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails

/**
 * EJSON `$date`. Every timestamp the app reads off the wire goes through this and it had no tests.
 */
class EDateTests {

    @Serializable
    data class Sample(val d: EDate)

    private val millis = 1_700_000_000_000L
    private val json = """{"d":{"${'$'}date":$millis}}"""

    @Test
    fun `serializes to a dollar-date object`() {
        assertEquals(json, Json.encodeToString(Sample(Instant.fromEpochMilliseconds(millis))))
    }

    @Test
    fun `deserializes from a dollar-date object`() {
        assertEquals(Instant.fromEpochMilliseconds(millis), Json.decodeFromString<Sample>(json).d)
    }

    @Test
    fun `round-trips`() {
        val original = Sample(Instant.fromEpochMilliseconds(millis))
        assertEquals(original, Json.decodeFromString<Sample>(Json.encodeToString(original)))
    }

    @Test
    fun `handles the epoch and pre-epoch dates`() {
        for (value in listOf(0L, -1L, -86_400_000L)) {
            val sample = Sample(Instant.fromEpochMilliseconds(value))
            assertEquals(sample, Json.decodeFromString<Sample>(Json.encodeToString(sample)))
        }
    }

    @Test
    fun `keeps millisecond precision`() {
        // Meteor dates are epoch millis; truncating to seconds would silently reorder messages
        // sent within the same second.
        val sample = Sample(Instant.fromEpochMilliseconds(1_700_000_000_123L))
        val decoded = Json.decodeFromString<Sample>(Json.encodeToString(sample))
        assertEquals(123, decoded.d.toEpochMilliseconds() % 1000)
    }

    @Test
    fun `rejects an object without a dollar-date key`() {
        assertFails { Json.decodeFromString<Sample>("""{"d":{"date":$millis}}""") }
    }

    @Test
    fun `rejects a bare number`() {
        assertFails { Json.decodeFromString<Sample>("""{"d":$millis}""") }
    }
}
