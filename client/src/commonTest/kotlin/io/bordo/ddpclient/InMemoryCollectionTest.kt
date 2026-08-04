package io.bordo.ddpclient

import io.bordo.ddpclient.db.memory.InMemoryCollection
import io.bordo.ddpclient.db.memory.addId
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * `InMemoryCollection` was only reached obliquely through `InMemoryDatabaseTest`; its merge and
 * removal semantics are what the whole minimongo mirror rests on.
 */
class InMemoryCollectionTest {

    private fun collection() = InMemoryCollection("users")

    private fun doc(vararg pairs: Pair<String, String>) = buildJsonObject {
        pairs.forEach { (k, v) -> put(k, JsonPrimitive(v)) }
    }

    @Test
    fun `putDocument stamps the id into the document`() {
        val collection = collection()
        collection.putDocument("1", doc("name" to "a"))

        val stored = collection.getDocument("1")
        assertEquals("1", stored?.get("_id")?.jsonPrimitive?.content)
        assertEquals("a", stored?.get("name")?.jsonPrimitive?.content)
    }

    @Test
    fun `putDocument replaces in place rather than appending a duplicate`() {
        val collection = collection()
        collection.putDocument("1", doc("name" to "a"))
        collection.putDocument("2", doc("name" to "b"))
        collection.putDocument("1", doc("name" to "c"))

        assertEquals(2, collection.documents.size)
        assertEquals(listOf("1", "2"), collection.documentIds)
        assertEquals("c", collection.getDocument("1")?.get("name")?.jsonPrimitive?.content)
    }

    @Test
    fun `putDocument fully replaces so absent fields are dropped`() {
        // put is a replace, not a merge -- this is the difference from updateDocument below.
        val collection = collection()
        collection.putDocument("1", doc("name" to "a", "surname" to "b"))
        collection.putDocument("1", doc("name" to "a"))

        assertNull(collection.getDocument("1")?.get("surname"))
    }

    @Test
    fun `updateDocument merges into the existing document`() {
        val collection = collection()
        collection.putDocument("1", doc("name" to "a", "surname" to "b"))
        collection.updateDocument("1", doc("surname" to "c"))

        val stored = collection.getDocument("1")
        assertEquals("a", stored?.get("name")?.jsonPrimitive?.content)
        assertEquals("c", stored?.get("surname")?.jsonPrimitive?.content)
    }

    @Test
    fun `updateDocument on a missing id is a no-op`() {
        val collection = collection()
        collection.putDocument("1", doc("name" to "a"))
        collection.updateDocument("missing", doc("name" to "z"))

        assertEquals(1, collection.documents.size)
        assertEquals("a", collection.getDocument("1")?.get("name")?.jsonPrimitive?.content)
    }

    @Test
    fun `removeFields drops only the named fields`() {
        val collection = collection()
        collection.putDocument("1", doc("name" to "a", "surname" to "b", "city" to "c"))
        collection.removeFields("1", listOf("surname", "city"))

        val stored = collection.getDocument("1")
        assertEquals("a", stored?.get("name")?.jsonPrimitive?.content)
        assertNull(stored?.get("surname"))
        assertNull(stored?.get("city"))
        assertEquals("1", stored?.get("_id")?.jsonPrimitive?.content)
    }

    @Test
    fun `removeFields tolerates unknown fields and a missing document`() {
        val collection = collection()
        collection.putDocument("1", doc("name" to "a"))

        collection.removeFields("1", listOf("nope"))
        collection.removeFields("missing", listOf("name"))

        assertEquals("a", collection.getDocument("1")?.get("name")?.jsonPrimitive?.content)
    }

    @Test
    fun `removeDocument removes one and ignores unknown ids`() {
        val collection = collection()
        collection.putDocument("1", doc("name" to "a"))
        collection.putDocument("2", doc("name" to "b"))

        collection.removeDocument("1")
        collection.removeDocument("missing")

        assertEquals(listOf("2"), collection.documentIds)
        assertNull(collection.getDocument("1"))
    }

    @Test
    fun `removeAll empties the collection`() {
        val collection = collection()
        repeat(5) { collection.putDocument("$it", doc("name" to "n$it")) }

        collection.removeAll()

        assertTrue(collection.documents.isEmpty())
        assertTrue(collection.documentIds.isEmpty())
    }

    @Test
    fun `documents with identical content are both removed by removeAll`() {
        // removeDocument matches by value (documents - newDocument), so duplicates that differ
        // only by _id must still each be found by their own id.
        val collection = collection()
        collection.putDocument("1", doc("name" to "same"))
        collection.putDocument("2", doc("name" to "same"))

        collection.removeAll()

        assertTrue(collection.documents.isEmpty())
    }

    @Test
    fun `getDocument returns null for an unknown id`() {
        assertNull(collection().getDocument("1"))
    }

    @Test
    fun `addId injects the id without disturbing other fields`() {
        val withId = doc("name" to "a").addId("42")

        assertEquals("42", withId["_id"]?.jsonPrimitive?.content)
        assertEquals("a", withId["name"]?.jsonPrimitive?.content)
    }

    @Test
    fun `addId overwrites an existing id`() {
        val withId = buildJsonObject {
            put("_id", JsonPrimitive("old"))
            put("name", JsonPrimitive("a"))
        }.addId("new")

        assertEquals("new", withId["_id"]?.jsonPrimitive?.content)
    }

    @Test
    fun `documentIds reflects insertion order`() {
        val collection = collection()
        listOf("c", "a", "b").forEach { collection.putDocument(it, doc("name" to it)) }

        assertEquals(listOf("c", "a", "b"), collection.documentIds)
        assertEquals("c", collection.documents.first().jsonObject["_id"]?.jsonPrimitive?.content)
    }
}
