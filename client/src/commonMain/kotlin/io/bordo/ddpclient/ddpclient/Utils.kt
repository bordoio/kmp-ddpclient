package io.bordo.ddpclient.ddpclient

import co.touchlab.kermit.Logger
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.converter
import io.ktor.client.plugins.websocket.receiveDeserialized
import io.ktor.serialization.WebsocketConverterNotFoundException
import io.ktor.serialization.WebsocketDeserializeException
import io.ktor.serialization.suitableCharset
import io.ktor.util.reflect.typeInfo
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.onClosed
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.onSuccess
import kotlin.random.Random
import kotlin.time.Clock

/**
 * Created by Osman Saral on 22.03.2023
 */
object Utils {
    fun generateSocketPath(): String {
        val serverId = Random.nextInt(100, 1000)
        val charPool = ('a'..'z') + ('A'..'Z') + ('0'..'9')
        val sessionId = (1..8)
            .map { Random.nextInt(0, charPool.size) }
            .map(charPool::get)
            .joinToString("")
        val transport = "websocket"

        return "sockjs/$serverId/$sessionId/$transport"
    }
}

@OptIn(DelicateCoroutinesApi::class)
suspend inline fun DefaultClientWebSocketSession.receiveAll(block: DefaultClientWebSocketSession.(Incoming) -> Unit) {
    while (true) {
        incoming.receiveCatching()
            .onSuccess { frame ->
                val converter = converter
                    ?: throw WebsocketConverterNotFoundException("No converter was found for websocket")

                if (!converter.isApplicable(frame)) {
                    throw WebsocketDeserializeException(
                        "Converter doesn't support frame type ${frame.frameType.name}",
                        frame = frame
                    )
                }
                val charset = call.request.headers.suitableCharset()
                val typeInfo = typeInfo<Incoming>()
                val result = converter.deserialize(
                    charset = charset,
                    typeInfo = typeInfo,
                    content = frame
                )

                val incomingMessage = when {
                    typeInfo.type.isInstance(result) -> result
                    result == null -> {
                        if (typeInfo.kotlinType?.isMarkedNullable == true) {
                            null
                        } else {
                            throw WebsocketDeserializeException("Frame has null content", frame = frame)
                        }
                    }
                    else -> throw WebsocketDeserializeException(
                        "Can't deserialize value: expected value of type ${typeInfo.type.simpleName}," +
                                " got ${result::class.simpleName}",
                        frame = frame
                    )
                } as Incoming


                block(incomingMessage)
            }
            .onClosed { cause ->
                Logger.d(tag = "CONNECTION_RETRY") { "Channel closed: ${cause?.message ?: "no cause"} $cause" }
                // Explicitly emit Close message when channel closes
                block(Incoming.Close)
                break // Exit the loop gracefully
            }
            .onFailure { error ->
                Logger.d(tag = "CONNECTION_RETRY") { "Receive failed: ${error?.message ?: "unknown error"}" }
                if (error != null) {
                    throw error
                } else {
                    // Channel closed without explicit error - treat as graceful close
                    Logger.d(tag = "CONNECTION_RETRY") { "Channel closed in onFailure without error" }
                    block(Incoming.Close)
                    break
                }
            }
    }
    Logger.d(tag = "CONNECTION_RETRY") { "receiveAll exited" }
}
