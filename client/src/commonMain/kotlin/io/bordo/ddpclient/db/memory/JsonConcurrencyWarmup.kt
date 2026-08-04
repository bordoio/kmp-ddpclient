package io.bordo.ddpclient.db.memory

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/**
 * Crash fix for the #4 iOS Crashlytics issue (EXC_BAD_ACCESS in
 * `kotlin.collections.HashMap.findKey` reached from
 * `JsonTreeDecoder.elementName` -> `HashMapKeys.contains`).
 *
 * Root cause (verified against kotlinx-serialization 1.9.0 + Kotlin/Native 2.2.10):
 *
 * DDP collection documents are parsed once (on the websocket reader) into [JsonObject]s and
 * stored here, then decoded with `json.decodeFromJsonElement<T>(document)` by several *concurrent*
 * background flows (e.g. `Database.receiveCollection`, `Database.getCollection`,
 * `GetSessionDetailUseCase`) that all read the SAME shared parsed document instances.
 *
 * While decoding an object, `JsonTreeDecoder.elementName` evaluates `baseName in value.keys`
 * (TreeJsonDecoder.kt:246). The first read of `value.keys` (and the first hash lookup) lazily
 * builds the Kotlin/Native `HashMap` keys-view / internal hash table on the *shared* document map.
 * Under the new Kotlin/Native memory model that lazy initialisation is not synchronised, so when
 * the first decode of a given document happens from several threads at once (a subscription
 * "ready" burst pushes many docs that are decoded in parallel) the lazy state races and corrupts
 * -> EXC_BAD_ACCESS.
 *
 * Note: the per-descriptor name caches (`Json.schemaCache`) are NOT the culprit here — on
 * Kotlin/Native that cache is `@ThreadLocal` (see kotlinx.serialization `JsonSchemaCache.kt`), so
 * it is built independently per thread and cannot be raced. The genuinely shared mutable-on-
 * first-read state is the parsed document's own map.
 *
 * Fix: as documents enter the store (single-threaded ingestion, before they are published to any
 * collector), eagerly materialise every map's lazy read structures by walking the whole element
 * tree and touching `keys` / `containsKey` / element access. After this, concurrent decoders only
 * ever perform read-only lookups against fully-initialised, immutable maps, which is safe. This
 * preserves full decode parallelism (no throttling) at a one-off, ingestion-time cost.
 */
internal fun JsonElement.materializeForConcurrentDecode() {
    when (this) {
        is JsonObject -> {
            // Touching `keys` builds the lazy keys-view; `containsKey` + element access force the
            // internal hash table to be built — exactly the lookups the tree decoder performs.
            for (key in keys) {
                containsKey(key)
                this[key]?.materializeForConcurrentDecode()
            }
        }

        is JsonArray -> {
            for (element in this) {
                element.materializeForConcurrentDecode()
            }
        }

        else -> {
            // JsonPrimitive / JsonNull have no lazily-initialised structures.
        }
    }
}
