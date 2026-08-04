package io.bordo.ddpclient.ejson

import kotlin.time.Instant
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

/**
 * Created by Osman Saral on 6.04.2023
 */

typealias EDate = @Serializable(EDateSerializer::class) Instant

@Serializable
@SerialName("EDate")
data class InstantSurrogate(
    @SerialName("\$date")
    val date: Long,
)

object EDateSerializer : KSerializer<Instant> {
    override val descriptor: SerialDescriptor = InstantSurrogate.serializer().descriptor

    override fun serialize(encoder: Encoder, value: Instant) {
        val millisecond = value.toEpochMilliseconds()
        val eDate = InstantSurrogate(millisecond)

        encoder.encodeSerializableValue(InstantSurrogate.serializer(), eDate)
    }

    override fun deserialize(decoder: Decoder): Instant {
        val eDate = decoder.decodeSerializableValue(InstantSurrogate.serializer())
        return Instant.fromEpochMilliseconds(eDate.date)
    }
}