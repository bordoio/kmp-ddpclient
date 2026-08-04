package io.bordo.ddpclient

import io.bordo.ddpclient.db.memory.InMemoryDatabase
import io.bordo.ddpclient.db.receiveCollection
import io.bordo.ddpclient.ddpclient.defaultJson
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class ReceiveCollectionTest {

    @Serializable
    data class Msg(val sessionId: String, val text: String, val createdAt: Long)

    private val json = defaultJson
    private val database = InMemoryDatabase(json)
    private val pageSize = 30

    /** Stands in for MessageFlowUseCase: sort, then truncate to the page window. */
    private fun pagedFlow(page: Int) =
        database.receiveCollection<Msg>("messages") { it.sessionId == "A" }
            .map { msgs -> msgs.sortedByDescending { it.createdAt }.take(page * pageSize) }

    /** [i] doubles as createdAt, so a higher index is a newer message. */
    private suspend fun add(i: Int, sessionId: String = "A") =
        database.onDataAdded("messages", "id$i", buildJsonObject {
            put("sessionId", sessionId); put("text", "m$i"); put("createdAt", i.toLong())
        })

    @Test
    // No `,` in the name: Kotlin/Native rejects it ("Name contains illegal characters")
    // and the whole native test binary fails to compile.
    fun `a page arriving as a burst emits once - not once per document`() = runTest {
        val emissions = mutableListOf<Int>()
        val job = launch { pagedFlow(page = 1).collect { emissions += it.size } }
        advanceUntilIdle()

        repeat(18) { add(it) }
        advanceUntilIdle()

        // The burst settles into a single list; no partial pages reach the UI (bubble flicker).
        assertContentEquals(listOf(0, 18), emissions)
        job.cancel()
    }

    @Test
    fun `a new document is emitted without any count changing`() = runTest {
        repeat(5) { add(it) }
        val ui = mutableListOf<Msg>()
        val job = launch { pagedFlow(page = 1).collect { ui.clear(); ui.addAll(it) } }
        advanceUntilIdle()
        assertEquals(5, ui.size)

        add(5)
        advanceUntilIdle()

        assertEquals(6, ui.size)
        assertEquals("m5", ui.first().text)
        job.cancel()
    }

    @Test
    fun `emits even when fewer documents arrive than the server claims exist`() = runTest {
        // The publication delivers 16 while session.messageCount says 17. This used to emit nothing
        // at all, freezing the list on stale data forever.
        repeat(16) { add(it) }
        val ui = mutableListOf<Msg>()
        val job = launch { pagedFlow(page = 1).collect { ui.clear(); ui.addAll(it) } }
        advanceUntilIdle()

        assertEquals(16, ui.size)
        job.cancel()
    }

    @Test
    fun `window keeps the newest documents when more are held than it fits`() = runTest {
        repeat(32) { add(it) } // 32 held locally, window is 30
        val ui = mutableListOf<Msg>()
        val job = launch { pagedFlow(page = 1).collect { ui.clear(); ui.addAll(it) } }
        advanceUntilIdle()

        assertEquals(30, ui.size)
        assertEquals("m31", ui.first().text) // newest kept
        assertEquals(false, ui.any { it.text == "m0" || it.text == "m1" }) // oldest dropped
        job.cancel()
    }

    @Test
    fun `filter keeps other sessions out of the window`() = runTest {
        repeat(3) { add(it, sessionId = "B") }
        repeat(2) { add(it + 10) }
        val ui = mutableListOf<Msg>()
        val job = launch { pagedFlow(page = 1).collect { ui.clear(); ui.addAll(it) } }
        advanceUntilIdle()

        assertEquals(2, ui.size)
        assertEquals(true, ui.all { it.sessionId == "A" })
        job.cancel()
    }

    /** The first implementation ended the flow once a page completed, freezing later updates. */
    @Test
    fun `updates keep flowing after the page window is already full`() = runTest {
        repeat(30) { add(it) }
        val ui = mutableListOf<Msg>()
        val job = launch { pagedFlow(page = 1).collect { ui.clear(); ui.addAll(it) } }
        advanceUntilIdle()
        assertEquals(30, ui.size)

        add(30)
        advanceUntilIdle()

        assertEquals(30, ui.size)
        assertEquals("m30", ui.first().text)
        job.cancel()
    }

    @Test
    fun `a document delivered twice is not duplicated`() = runTest {
        val ui = mutableListOf<Msg>()
        val job = launch { pagedFlow(page = 1).collect { ui.clear(); ui.addAll(it) } }
        advanceUntilIdle()

        add(1)
        advanceUntilIdle()
        add(1) // same _id delivered again
        database.onDataChanged("messages", "id1", buildJsonObject { put("text", "edited") }, null)
        advanceUntilIdle()

        assertEquals(1, ui.size)
        assertEquals("edited", ui.single().text)
        job.cancel()
    }

    /** Deduplicating by value (the old mutableSetOf) would collapse these two into one. */
    @Test
    fun `two messages that decode to identical content are both kept`() = runTest {
        repeat(2) { i ->
            database.onDataAdded("messages", "same$i", buildJsonObject {
                put("sessionId", "A"); put("text", "ok"); put("createdAt", 100L)
            })
        }
        val ui = mutableListOf<Msg>()
        val job = launch { pagedFlow(page = 1).collect { ui.clear(); ui.addAll(it) } }
        advanceUntilIdle()

        assertEquals(2, ui.size)
        job.cancel()
    }

    @Test
    fun `a filter matching nothing emits an empty list`() = runTest {
        repeat(3) { add(it, sessionId = "B") }
        val ui = mutableListOf<List<Msg>>()
        val job = launch { pagedFlow(page = 1).collect { ui += it } }
        advanceUntilIdle()

        assertEquals(listOf(emptyList()), ui)
        job.cancel()
    }

    @Test
    fun `page 2 widens the window`() = runTest {
        repeat(45) { add(it) }
        val ui = mutableListOf<Msg>()
        val job = launch { pagedFlow(page = 2).collect { ui.clear(); ui.addAll(it) } }
        advanceUntilIdle()

        assertEquals(45, ui.size)
        job.cancel()
    }
}
