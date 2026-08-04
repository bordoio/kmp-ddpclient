package io.bordo.ddpclient.ddpclient

import io.ktor.http.HttpStatusCode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

@Suppress("SERIALIZER_TYPE_INCOMPATIBLE")
@Serializable
sealed class Outgoing(val msg: String) {
    @Serializable
    @SerialName("Connect")
    data class Connect(
        val session: String? = null,
        val version: String,
        val support: List<String>,
    ) : Outgoing("connect")

    @Serializable
    @SerialName("Ping")
    data class Ping(
        val id: String?,
    ) : Outgoing("ping")

    @Serializable
    @SerialName("Pong")
    data class Pong(
        val id: String?,
    ) : Outgoing("pong")

    @Serializable
    @SerialName("Subscribe")
    data class Subscribe(
        val id: String,
        val name: String,
        val params: JsonArray?,
    ) : Outgoing("sub")

    @Serializable
    @SerialName("Unsubscribe")
    data class Unsubscribe(
        val id: String,
    ) : Outgoing("unsub")

    @Serializable
    @SerialName("Method")
    data class Method(
        val method: String,
        val params: JsonArray?,
        val id: String,
        val randomSeed: String?,
    ) : Outgoing("method")
}

@Serializable
sealed class Incoming(val msg: String) {
    // Connection Messages
    @Serializable
    data class Connected(
        val session: String?,
    ) : Incoming("connected")

    @Serializable
    data class Failed(
        val version: String,
    ) : Incoming("failed")

    @Serializable
    data class Ping(
        val id: String?,
    ) : Incoming("ping")

    @Serializable
    data class Pong(
        val id: String?,
    ) : Incoming("pong")

    // Subscription Data Responses
    @Serializable
    data class Added(
        val collection: String,
        val id: String,
        val fields: JsonObject,
    ) : Incoming("added")

    @Serializable
    data class Changed(
        val collection: String,
        val id: String,
        val fields: JsonObject?,
        val cleared: List<String>?,
    ) : Incoming("changed")

    @Serializable
    data class Removed(
        val collection: String,
        val id: String,
    ) : Incoming("removed")

    // Subscription Responses
    @Serializable
    data class NoSub(
        val id: String,
        val error: ResponseError?,
    ) : Incoming("nosub"),
        SubscriptionResponseMessage

    @Serializable
    data class Ready(
        val subs: List<String>,
    ) : Incoming("ready"),
        SubscriptionResponseMessage

    // Method Responses
    @Serializable
    data class Result(
        val id: String,
        val error: ResponseError?,
        val result: JsonElement?,
    ) : Incoming("result"),
        MethodResponseMessage

    @Serializable
    data class Updated(
        val methods: List<String>,
    ) : Incoming("updated"),
        MethodResponseMessage

    // Generic responses
    @Serializable
    data class Error(
        val reason: String,
        val offendingMessage: JsonObject?,
    ) : Incoming("error")

    data class Exception(
        val exception: Throwable,
    ) : Incoming("exception")

    // sock.js messages
    @Serializable
    data object Open : Incoming("open")

    @Serializable
    data object Close : Incoming("close")

    @Serializable
    data object Heartbeat : Incoming("heartbeat")

    @Serializable
    data object Message : Incoming("message")

    sealed interface SubscriptionResponseMessage
    sealed interface MethodResponseMessage
}

@Serializable
data class ResponseError(
    val error: String,
    val reason: String,
    val message: String,
    val errorType: String,
    val details: JsonElement? = null
) {
    companion object {
        val Default = ResponseError(
            "generic",
            "generic",
            "An error occurred",
            "generic"
        )

        val UpdateRequired = ResponseError(
            "update_required",
            "update_required",
            "Update is required",
            "update_required"
        )

        fun fromHttpError(statusCode: HttpStatusCode) = ResponseError(
            statusCode.toString(),
            "generic",
            statusCode.description,
            "http_error"
        )
    }
}