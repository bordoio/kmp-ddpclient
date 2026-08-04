package io.bordo.ddpclient.ejson

import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonTransformingSerializer
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * Created by Osman Saral on 6.04.2023
 */

typealias EBinary = @Serializable(EBinarySerializer::class) String

// TODO: we can add bitmap (KorIm) and string serializers respectively for binary
object EBinarySerializer : JsonTransformingSerializer<String>(String.serializer()) {
    override fun transformSerialize(element: JsonElement): JsonElement {
        require(element is JsonPrimitive)
        val binary = element.jsonPrimitive.content

        return buildJsonObject {
            put("\$binary", JsonPrimitive(binary))
        }
    }

    override fun transformDeserialize(element: JsonElement): JsonElement {
        val eBinary = element.jsonObject["\$binary"]?.jsonPrimitive?.content ?: throw IllegalStateException("EBinary object should have \$binary")

        return JsonPrimitive(eBinary)
    }
}
