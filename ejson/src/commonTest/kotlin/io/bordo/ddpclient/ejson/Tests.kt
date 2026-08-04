package io.bordo.ddpclient.ejson

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails

/**
 * Created by Osman Saral on 11.04.2023
 */
class Tests {

    @Serializable
    data class EBinarySample(val b: EBinary)

    @Test
    fun `should be able to serialize EBinary`() {
        val binary = EBinarySample("test")
        val j = Json.encodeToString(binary)

        assertEquals("{\"b\":{\"\$binary\":\"test\"}}", j)
    }

    @Test
    fun `should be able to deserialize EBinary`() {
        val json = "{\"b\":{\"\$binary\":\"test\"}}"
        val binary = Json.decodeFromString<EBinarySample>(json)

        assertEquals(EBinarySample("test"), binary)
    }

    @Test
    fun `should throw error if not a binary object`() {
        val json = "{\"b\":{\"binary\":\"test\"}}"

        assertFails {
            Json.decodeFromString<EBinarySample>(json)
        }
    }
}
