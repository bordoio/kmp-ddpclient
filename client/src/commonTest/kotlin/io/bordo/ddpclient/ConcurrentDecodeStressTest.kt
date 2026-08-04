package io.bordo.ddpclient

import io.bordo.ddpclient.db.getCollection
import io.bordo.ddpclient.db.memory.InMemoryDatabase
import io.bordo.ddpclient.ddpclient.defaultJson
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Regression test for the #4 iOS crash (EXC_BAD_ACCESS in HashMap.findKey via
 * JsonTreeDecoder.elementName) caused by several background flows tree-decoding the SAME shared
 * parsed DDP documents concurrently on Kotlin/Native.
 *
 * Documents are added to the database (single-threaded ingestion, like the websocket reader) and
 * then decoded from many coroutines on the multi-threaded [Dispatchers.Default], mirroring the
 * subscription-burst -> parallel decode pattern. With the ingestion-time materialization in place
 * the shared maps are fully built before any concurrent read, so this completes cleanly; without
 * it the first concurrent `value.keys` lookup races and corrupts the map.
 */
class ConcurrentDecodeStressTest {

    @Serializable
    data class Profile(val displayName: String, val avatar: String, val locale: String)

    @Serializable
    data class User(val id: String, val name: String, val profile: Profile)

    @Serializable
    data class Search(val query: String, val user: User)

    @Serializable
    data class SessionDoc(val _id: String, val title: String, val search: Search)

    private fun sampleDocument(index: Int): JsonObject = buildJsonObject {
        put("title", "session-$index")
        put("search", buildJsonObject {
            put("query", "q-$index")
            put("user", buildJsonObject {
                put("id", "user-$index")
                put("name", "name-$index")
                put("profile", buildJsonObject {
                    put("displayName", "display-$index")
                    put("avatar", "avatar-$index")
                    put("locale", "en")
                })
            })
        })
    }

    @Test
    fun `concurrent decode of shared nested documents does not crash`() = runBlocking {
        val json = defaultJson
        val database = InMemoryDatabase(json)

        val documentCount = 200
        repeat(documentCount) { index ->
            // Ingestion happens single-threaded, before any concurrent decode (as in production).
            database.onDataAdded("sessions", "session-$index", sampleDocument(index))
        }

        // Hammer the SAME shared documents from many coroutines on the multithreaded default
        // dispatcher, repeatedly, to surface any first-touch lazy-init race.
        val readers = 32
        val iterationsPerReader = 25
        coroutineScope {
            (0 until readers).map {
                async(Dispatchers.Default) {
                    repeat(iterationsPerReader) {
                        val sessions = database.getCollection<SessionDoc>("sessions")
                        assertEquals(documentCount, sessions?.size)
                    }
                }
            }.awaitAll()
        }
        Unit
    }
}
