package io.bordo.ddpclient.ejson

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

/**
 * Created by Osman Saral on 6.04.2023
 */

typealias ERegex = @Serializable(ERegexSerializer::class) Regex

@Serializable
@SerialName("ERegex")
data class ERegexSurrogate(
    @SerialName("\$regexp")
    val pattern: String,
    @SerialName("\$flags")
    val flags: String,
)

object ERegexSerializer : KSerializer<Regex> {
    override val descriptor: SerialDescriptor = ERegexSurrogate.serializer().descriptor

    override fun serialize(encoder: Encoder, value: Regex) {
        val eRegex = ERegexSurrogate(value.pattern, "") // TODO: map flags

        encoder.encodeSerializableValue(ERegexSurrogate.serializer(), eRegex)
    }

    override fun deserialize(decoder: Decoder): Regex {
        val eRegex = decoder.decodeSerializableValue(ERegexSurrogate.serializer())
        return Regex(eRegex.pattern)
    }
}
