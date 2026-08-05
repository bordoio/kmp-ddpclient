package io.bordo.ddpclient.db.memory

import io.bordo.ddpclient.db.Database
import io.bordo.ddpclient.db.DbCollection
import io.bordo.ddpclient.ddpclient.defaultJson
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject

/** Database that is stored in memory  */
class InMemoryDatabase(override val json: Json) : Database {
    override val collections: MutableMap<String, DbCollection> = mutableMapOf()

    private val collectionFlows: MutableMap<String, MutableSharedFlow<DbCollection>> = mutableMapOf()

    override fun getCollectionFlow(name: String): MutableSharedFlow<DbCollection> {
        if (name.isEmpty()) {
            throw IllegalArgumentException("name cannot be empty")
        }
        if (!collectionFlows.containsKey(name)) {
            collectionFlows.getOrPut(name) { MutableSharedFlow(replay = 10) }
                .tryEmit(InMemoryCollection(name))
        }

        return collectionFlows[name]!!
    }

    private fun getOrAddCollection(name: String): DbCollection {
        if (!collections.containsKey(name)) {
            collections[name] = InMemoryCollection(name)
        }

        return collections[name]!!
    }

    override fun dump(): String {
        val pretty = Json(from = defaultJson) {
            prettyPrint = true
        }

        // Dump the documents as the JSON tree they already are. Encoding `collections` directly
        // asked for a serializer for the DbCollection *interface*, which has no registered
        // subclasses, so every call threw SerializationException. Going through JsonObject also
        // keeps dump() free of any serialization requirement on custom DbCollection types.
        return pretty.encodeToString(
            JsonObject(collections.mapValues { (_, collection) -> collection.documents }),
        )
    }

    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    override fun drop() {
        collections.clear()
        // Also clear each collection flow's replay cache and push an empty snapshot. Otherwise the
        // replay=10 buffer keeps re-delivering stale documents after a logout / tenant switch, so a
        // newly attached collector would see data from the previous session.
        collectionFlows.forEach { (name, flow) ->
            flow.resetReplayCache()
            flow.tryEmit(InMemoryCollection(name))
        }
    }

    override suspend fun onDataAdded(
        collectionName: String,
        documentID: String,
        newValues: JsonObject,
    ) {
        val collection = getOrAddCollection(collectionName)
            .apply {
                putDocument(documentID, newValues)
            }

        getCollectionFlow(collectionName).emit(collection)
    }

    override suspend fun onDataChanged(
        collectionName: String,
        documentID: String,
        updatedValues: JsonObject?,
        removedFields: List<String>?,
    ) {
        val collection = getOrAddCollection(collectionName)
            .apply {
                if (updatedValues != null) {
                    if (documentIds.indexOf(documentID) < 0) {
                        onDataAdded(collectionName, documentID, updatedValues)
                        return
                    }

                    updateDocument(documentID, updatedValues)
                }

                if (removedFields != null) {
                    removeFields(documentID, removedFields)
                }
            }
        getCollectionFlow(collectionName).emit(collection)
    }

    override suspend fun onDataRemoved(collectionName: String, documentID: String) {
        val collection = getOrAddCollection(collectionName).apply {
            removeDocument(documentID)
        }

        val flow = getCollectionFlow(collectionName)
        flow.emit(collection)
    }
}
