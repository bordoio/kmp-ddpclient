package io.bordo.ddpclient.db

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject

/**
 * Created by Osman Saral on 3.04.2023
 */

interface DbCollection {
    /**
     * Returns the name of the collection
     *
     * @return the name
     */
    val name: String

    /**
     * Returns the name of the collection
     *
     * @return the name
     */
    var documents: JsonArray

    /**
     * Returns the document with the specified ID from the collection
     *
     * @param id the ID of the document to return
     * @return the document object or `null`
     */
    fun getDocument(id: String): JsonObject?

    /**
     * Puts the document with the specified ID to the collection
     *
     * @param id the ID of the document to put
     * @param document new document to be put to the collection
     */
    fun putDocument(id: String, document: JsonObject)

    /**
     * Updates the document with the specified ID in the collection.
     *
     * @param id the ID of the document to update
     * @param document new json fields to update the old document. the new fields will be added to the old document if they don't exist, or will be update the existing fields.
     */
    fun updateDocument(id: String, document: JsonObject)

    /**
     * Removes fields from the document with the specified ID
     *
     * @param id the ID of the document to return
     */
    fun removeFields(id: String, fields: List<String>)

    /**
     * Removes the document with the specified ID from the collection
     *
     * @param id the ID of the document to return
     */
    fun removeDocument(id: String)

    /**
     * Removes all documents from the collection
     *
     */
    fun removeAll()

    /**
     * Lists all documents from the collection by returning a set of their IDs
     *
     * @return an array containing the IDs of all documents
     */
    val documentIds: List<String>
}
