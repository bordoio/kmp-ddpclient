package io.bordo.ddpclient.db

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement

/*
 * Copyright (c) delight.im <info@delight.im>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */ /** Storage for data that exposes both read and write access  */
interface Database : DataStore {

    val json: Json

    val collections: Map<String, DbCollection>

    /**
     * Returns the collection with the specified name from the database
     *
     * The collection may or may not actually exist
     *
     * If the collection does not exist, an empty collection is implicitly created
     *
     * @param name the name of the collection to return
     * @return a collection object (never `null`)
     */
    fun getCollectionFlow(name: String): SharedFlow<DbCollection>

    /**
     * Lists all collections from the database by returning a set of their names
     *
     * @return an array containing the names of all collections
     */
    val collectionNames: List<String>
        get() = collections.keys.toList()

    fun dump(): String

    fun drop()
}

inline fun <reified T> Database.getCollection(name: String): List<T>? =
    collections[name]?.documents?.map { document ->
        json.decodeFromJsonElement<T>(document)
    }

fun Database.getRawCollection(name: String): JsonArray? = collections[name]?.documents

inline fun <reified T> Database.receiveCollection(name: String): Flow<List<T>> =
    getCollectionFlow(name).map { collection ->
        collection.documents.map { document ->
            json.decodeFromJsonElement(document)
        }
    }

fun Database.receiveRawCollection(name: String): Flow<JsonArray> =
    getCollectionFlow(name).map { collection ->
        collection.documents
    }

/**
 * How long a burst of DDP updates for [receiveCollection] must go quiet before the collection is
 * considered settled.
 */
const val BURST_SETTLE_MILLIS = 50L

/**
 * Emits the documents of [name] matching [filter], re-emitting whenever the collection changes.
 *
 * A page arrives as a burst of one `added` per document, so emitting on every change would show the
 * list growing a row at a time. Waiting [BURST_SETTLE_MILLIS] for the burst to settle collapses it
 * into a single emission of the finished list.
 *
 * Callers that page must sort *before* truncating to their window, so the window keeps the newest
 * documents rather than whichever ones DDP happened to insert first.
 */
@OptIn(FlowPreview::class)
inline fun <reified T> Database.receiveCollection(
    name: String,
    crossinline filter: (T) -> Boolean
): Flow<List<T>> =
    getCollectionFlow(name)
        // ponytail: debounce never fires while updates keep arriving closer together than
        // BURST_SETTLE_MILLIS. Human-paced traffic settles; switch to sample() if a busy session
        // ever stalls the list.
        .debounce(BURST_SETTLE_MILLIS)
        .map { collection ->
            collection.documents.map<JsonElement, T> { document ->
                json.decodeFromJsonElement<T>(document)
            }.filter(filter)
        }
        .distinctUntilChanged()
