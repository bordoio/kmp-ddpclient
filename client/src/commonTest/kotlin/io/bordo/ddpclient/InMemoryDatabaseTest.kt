package io.bordo.ddpclient

import app.cash.turbine.TurbineTestContext
import app.cash.turbine.test
import io.bordo.ddpclient.db.DbCollection
import io.bordo.ddpclient.db.memory.InMemoryDatabase
import io.bordo.ddpclient.db.memory.addId
import io.bordo.ddpclient.db.receiveCollection
import io.bordo.ddpclient.ddpclient.defaultJson
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class InMemoryDatabaseTest {

    @Serializable
    data class User(val name: String, val surname: String)

    private val json = defaultJson
    private val database = InMemoryDatabase(json)

    private val jsonUser = json.encodeToJsonElement(User("foo", "bar")).jsonObject

    @Test
    fun `should add id to json object`() {
        val newUser = jsonUser.addId("123")

        assertEquals("123", newUser["_id"]?.jsonPrimitive?.content)
    }

    @Test
    fun `when data added to the database collection size should be 1`() = runBlocking {
        database.getCollectionFlow("users").test {
            assertEmptyCollection()
            database.onDataAdded("users", "1", jsonUser)
            val collection = awaitItem()

            assertEquals(1, collection.documents.size)

            database.onDataAdded("users", "2", jsonUser)

            assertEquals(2, collection.documents.size)
            val expected = jsonUser.addId("1")
            assertEquals(expected, collection.documents[0])
            assertEquals("1", collection.documents[0].jsonObject["_id"]?.jsonPrimitive?.content)

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `when data added to the database with same id it should be replaced`() = runBlocking {
        val userList = mapOf(
            "1" to User("Alice", "bar"),
            "2" to User("Bob", "bar"),
            "3" to User("Charlie", "bar"),
        )

        val jsonUsers = userList.mapValues {
            json.encodeToJsonElement(it.value).jsonObject
        }

        database.getCollectionFlow("users").test {
            assertEmptyCollection()
            for (jsonUser in jsonUsers) {
                database.onDataAdded("users", jsonUser.key, jsonUser.value)
            }

            assertEquals("Alice", awaitItem().documents[0].jsonObject["name"]?.jsonPrimitive?.content)
            assertEquals("Bob", awaitItem().documents[1].jsonObject["name"]?.jsonPrimitive?.content)
            assertEquals("Charlie", awaitItem().documents[2].jsonObject["name"]?.jsonPrimitive?.content)

            val newUser = User("Bobby", "bar")

            database.onDataAdded("users", "2", json.encodeToJsonElement(newUser).jsonObject)

            val collection = awaitItem()
            assertEquals(3, collection.documents.size)

            assertEquals("Bobby", collection.documents[1].jsonObject["name"]?.jsonPrimitive?.content)

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `when a data updated it collectors should be notified`() = runBlocking {
        runTest {
            database.getCollectionFlow("users").test {
                assertEmptyCollection()
                database.onDataAdded("users", "1", jsonUser)
                database.onDataAdded("users", "2", jsonUser)
                database.onDataAdded("users", "3", jsonUser)
                val collection = awaitItem()

                assertEquals(3, collection.documents.size)

                database.onDataChanged(
                    "users",
                    "2",
                    buildJsonObject { put("surname", JsonPrimitive("baz")) },
                    null,
                )

                cancelAndIgnoreRemainingEvents()
            }
        }

        runTest {
            database.receiveCollection<User>("users").test {
                assertEquals(0, awaitItem().size)
                assertEquals(3, awaitItem().size)

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `when a data is empty receive collection should emit it`() = runBlocking {
        runTest {
            database.receiveCollection<User>("users").test {
                assertEquals(0, awaitItem().size)

                cancelAndIgnoreRemainingEvents()
            }
        }
    }

    @Test
    fun `when a data updated it should be inserted if id doesn't exists`() = runBlocking {
        database.getCollectionFlow("users").test {
            assertEmptyCollection()
            database.onDataChanged("users", "1", jsonUser, null)
            val collection = awaitItem()

            assertEquals(1, collection.documents.size)
            val expected = jsonUser.addId("1")
            assertEquals(expected, collection.documents[0])

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `when a data updated only the new fields should be updated`() = runBlocking {
        database.getCollectionFlow("users").test {
            assertEmptyCollection()
            database.onDataAdded("users", "1", jsonUser)
            val collection = awaitItem()

            assertEquals(1, collection.documents.size)
            val expected = jsonUser.addId("1")
            assertEquals(expected, collection.documents[0])

            database.onDataChanged(
                "users",
                "1",
                buildJsonObject { put("name", JsonPrimitive("baz")) },
                null,
            )
            val newDocument = awaitItem().getDocument("1")

            assertNotNull(newDocument)

            val surname = newDocument["surname"]?.jsonPrimitive?.content
            assertEquals("bar", surname, "surname shouldn't change")

            val name = newDocument["name"]?.jsonPrimitive?.content
            assertEquals("baz", name, "name should be changed")

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `when a change message is received with removed fields they should be removed`() =
        runBlocking {
            database.getCollectionFlow("users").test {
                assertEmptyCollection()
                database.onDataAdded("users", "1", jsonUser)
                val collection = awaitItem()

                assertEquals(1, collection.documents.size)
                val expected = jsonUser.addId("1")
                assertEquals(expected, collection.documents[0])

                database.onDataChanged("users", "1", null, listOf("name"))
                val newDocument = awaitItem().getDocument("1")

                assertNotNull(newDocument)

                val surname = newDocument["surname"]?.jsonPrimitive?.content
                assertEquals("bar", surname, "surname shouldn't change")

                assertFalse(newDocument.containsKey("name"), "name should be removed")

                cancelAndIgnoreRemainingEvents()
            }
        }

    @Test
    fun `when a remove message is received item should be removed`() = runBlocking {
        database.getCollectionFlow("users").test {
            assertEmptyCollection()
            database.onDataAdded("users", "1", jsonUser)
            val collection = awaitItem()

            assertEquals(1, collection.documents.size)
            val expected = jsonUser.addId("1")
            assertEquals(expected, collection.documents[0])

            database.onDataRemoved("users", "1")

            val newCollection = awaitItem()
            assertEquals(0, collection.documents.size)

            val newDocument = newCollection.getDocument("1")

            assertNull(newDocument)

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `adding data to a collection should update the collection's documentIds`() = runBlocking {
        database.getCollectionFlow("users").test {
            assertEmptyCollection()
            database.onDataAdded("users", "1", jsonUser)

            assertEquals(listOf("1"), awaitItem().documentIds)

            database.onDataAdded("users", "2", jsonUser)

            assertEquals(listOf("1", "2"), awaitItem().documentIds)

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `removing a document from a collection should update the collection's documentIds`() = runBlocking {
        database.getCollectionFlow("users").test {
            assertEmptyCollection()
            database.onDataAdded("users", "1", jsonUser)
            database.onDataAdded("users", "2", jsonUser)

            database.onDataRemoved("users", "1")

            assertEquals(listOf("2"), expectMostRecentItem().documentIds)

            cancelAndIgnoreRemainingEvents()
        }
    }

    @Test
    fun `receiving a collection with an empty name should throw IllegalArgumentException`() {
        assertFailsWith<IllegalArgumentException> {
            database.getCollectionFlow("")
        }
    }

    @Test
    fun `dump returns the documents as parseable json`() = runTest {
        database.onDataAdded("users", "u1", jsonUser)
        database.onDataAdded("orders", "o1", buildJsonObject { put("total", JsonPrimitive(7)) })

        // Regression: dump() encoded Map<String, DbCollection>, which needs a serializer for the
        // DbCollection interface -- it had no registered subclasses, so this threw
        // SerializationException for every caller.
        val dumped = json.parseToJsonElement(database.dump()).jsonObject

        assertEquals("foo", dumped["users"]!!.jsonArray.single().jsonObject["name"]?.jsonPrimitive?.content)
        assertEquals("u1", dumped["users"]!!.jsonArray.single().jsonObject["_id"]?.jsonPrimitive?.content)
        assertEquals(7, dumped["orders"]!!.jsonArray.single().jsonObject["total"]?.jsonPrimitive?.int)
    }

    @Test
    fun `dump of an empty database is still valid json`() {
        assertEquals(0, json.parseToJsonElement(database.dump()).jsonObject.size)
    }

    private suspend fun TurbineTestContext<DbCollection>.assertEmptyCollection() {
        assertEquals(0, awaitItem().documents.size)
    }
}
