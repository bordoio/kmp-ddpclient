package io.bordo.ddpclient.random

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MeteorRandomTest {

    private val unmistakable = "23456789ABCDEFGHJKLMNPQRSTWXYZabcdefghijkmnopqrstuvwxyz"

    @Test
    fun sameSeedProducesSameId() {
        val seeds = arrayOf<Any>("58718e1458ee04c96a48", "/collection/messages")
        val a = MeteorRandom.createWithSeeds(*seeds).id()
        val b = MeteorRandom.createWithSeeds(*seeds).id()
        assertEquals(a, b, "Two generators with identical seeds must produce identical ids")
    }

    @Test
    fun idHasDefaultLengthAndAlphabet() {
        val id = MeteorRandom.createWithSeeds("seed", "/collection/messages").id()
        assertEquals(17, id.length)
        assertTrue(id.all { it in unmistakable }, "id must only use the unmistakable alphabet")
    }

    @Test
    fun differentSeedsProduceDifferentIds() {
        val a = MeteorRandom.createWithSeeds("a", "/collection/messages").id()
        val b = MeteorRandom.createWithSeeds("b", "/collection/messages").id()
        assertTrue(a != b)
    }

    /**
     * Regression guard locking the ported algorithm to a known output. These vectors
     * were produced by this port; once verified against the real Meteor backend they
     * also confirm server/client parity. If this fails, the Alea/Mash port drifted.
     */
    @Test
    fun goldenVectors() {
        assertEquals(
            EXPECTED_MESSAGES_ID,
            MeteorRandom.createWithSeeds("58718e1458ee04c96a48", "/collection/messages").id()
        )
    }

    private companion object {
        // TODO: replace with an _id captured from the real Meteor server for the same
        //  randomSeed to assert true client/server parity.
        const val EXPECTED_MESSAGES_ID = "pzM6faeMPgyqEgKB6"
    }
}
