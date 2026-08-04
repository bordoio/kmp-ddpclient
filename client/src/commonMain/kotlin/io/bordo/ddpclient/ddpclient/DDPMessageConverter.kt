package io.bordo.ddpclient.ddpclient

import io.bordo.ddpclient.util.clearMessage
import co.touchlab.kermit.Logger
import io.ktor.serialization.WebsocketContentConverter
import io.ktor.util.reflect.TypeInfo
import io.ktor.utils.io.charsets.Charset
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.time.Clock

/**
 * Created by Osman Saral on 27.03.2023
 */
class DDPMessageConverter(
    private val json: Json,
) : WebsocketContentConverter {

    override suspend fun serialize(
        charset: Charset,
        typeInfo: TypeInfo,
        value: Any?,
    ): Frame {
        if (value !is Outgoing) return Frame.Text("")
        val message = encodeToDDPMessage(value)

        Logger.i(tag = "DDPClient") { "Client is sending message: $message time: ${Clock.System.now()}" }
        return Frame.Text(message)
    }

    override suspend fun deserialize(charset: Charset, typeInfo: TypeInfo, content: Frame): Any {
        if (typeInfo.type != Incoming::class) {
            throw IllegalArgumentException("Can't deserialize types other than Incoming")
        }
        if (content !is Frame.Text) {
            throw IllegalStateException("Can't deserialize non Text Frames")
        }

        val message = content.readText()

        Logger.i(tag = "DDPClient") { "Client received message: $message  time: ${Clock.System.now()}" }

        if (message.startsWith("o")) return Incoming.Open
        if (message.startsWith("c")) return Incoming.Close
        if (message.startsWith("h")) return Incoming.Heartbeat
        if (message.startsWith("m")) return Incoming.Message

        if (!message.startsWith("a[")) {
            throw IllegalStateException("Can't deserialize message $message")
        }

        val jsonString = clearMessage(message)

        val jsonObject = try {
            json.parseToJsonElement(jsonString).jsonObject
        } catch (e: Exception) {
            Logger.e(tag = "DDPClient") { "Cannot parse $jsonString" }
            Logger.e(tag = "DDPClient", throwable = e, messageString = "Exception" )
            throw e
        }

        return when (val msg = jsonObject["msg"]?.jsonPrimitive?.content) {
            "connected" -> json.decodeFromString<Incoming.Connected>(jsonString)
            "failed" -> json.decodeFromString<Incoming.Failed>(jsonString)
            "ping" -> json.decodeFromString<Incoming.Ping>(jsonString)
            "pong" -> json.decodeFromString<Incoming.Pong>(jsonString)
            "added" -> json.decodeFromString<Incoming.Added>(jsonString)
            "changed" -> json.decodeFromString<Incoming.Changed>(jsonString)
            "removed" -> json.decodeFromString<Incoming.Removed>(jsonString)
            "ready" -> json.decodeFromString<Incoming.Ready>(jsonString)
            "nosub" -> json.decodeFromString<Incoming.NoSub>(jsonString)
            "error" -> json.decodeFromString<Incoming.Error>(jsonString)
            "result" -> json.decodeFromString<Incoming.Result>(jsonString)
            "updated" -> json.decodeFromString<Incoming.Updated>(jsonString)
            else -> throw IllegalStateException("Can't deserialize message $msg type yet")
        }
    }

    override fun isApplicable(frame: Frame): Boolean {
        return frame is Frame.Text
    }

    private fun encodeToDDPMessage(message: Outgoing): String {
        val array = arrayOf(json.encodeToString(message))
        return json.encodeToString(array)
    }
}
