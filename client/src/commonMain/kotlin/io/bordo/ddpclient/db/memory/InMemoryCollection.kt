package io.bordo.ddpclient.db.memory

import io.bordo.ddpclient.db.DbCollection
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/**
 * Created by Osman Saral on 4.04.2023
 */

data class InMemoryCollection(
    override val name: String,
) : DbCollection {
    override var documents: JsonArray = JsonArray(listOf())

    override fun getDocument(id: String) =
        documents.firstOrNull { it.jsonObject["_id"]?.jsonPrimitive?.contentOrNull == id }?.jsonObject

    override fun putDocument(id: String, document: JsonObject) {
        val newDocument = document.addId(id)
        // Build the parsed map's lazy structures now, single-threaded, before this document is
        // exposed to the concurrent background decoders. See materializeForConcurrentDecode.
        newDocument.materializeForConcurrentDecode()

        val currentIndex = documents.indexOfFirst { it.jsonObject["_id"]?.jsonPrimitive?.contentOrNull == id }

        if (currentIndex != -1) {
            replaceDocument(currentIndex, newDocument)
        } else {
            documents = JsonArray(documents + newDocument)
        }
    }

    private fun replaceDocument(index: Int, document: JsonObject) {
        val newDocumentList = documents.toMutableList()
        newDocumentList[index] = document
        documents = JsonArray(newDocumentList)
    }

    override fun updateDocument(id: String, document: JsonObject) {
        val oldDocument = getDocument(id) ?: return
        val newDocument = JsonObject(oldDocument + document)
        internalUpdateDocument(id, newDocument)
    }

    override fun removeFields(id: String, fields: List<String>) {
        val document = getDocument(id)?.toMutableMap() ?: return

        for (field in fields) {
            document.remove(field)
        }

        internalUpdateDocument(id, JsonObject(document))
    }

    override fun removeDocument(id: String) {
        val newDocument = getDocument(id) ?: return
        documents = JsonArray(documents - newDocument)
    }

    override fun removeAll() {
        documentIds.forEach {
            removeDocument(it)
        }
    }

    private fun internalUpdateDocument(id: String, newDocument: JsonObject) {
        // Same rationale as putDocument: materialize the (re)built document's lazy map structures
        // on this ingestion thread before it becomes visible to concurrent decoders.
        newDocument.materializeForConcurrentDecode()

        val documentList = documents.map { it.jsonObject }.toMutableList()
        val index = documentList.indexOfFirst { it["_id"]?.jsonPrimitive?.contentOrNull == id }

        if (index < 0) return

        documentList[index] = newDocument

        documents = JsonArray(documentList)
    }

    override val documentIds: List<String>
        get() = documents.mapNotNull { it.jsonObject["_id"]?.jsonPrimitive?.content }

    override fun toString(): String {
        return super.toString() + " documents: $documents"
    }
}

fun JsonObject.addId(id: String) = JsonObject(this + buildJsonObject { put("_id", id) })
