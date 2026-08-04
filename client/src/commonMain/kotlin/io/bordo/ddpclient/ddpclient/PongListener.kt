package io.bordo.ddpclient.ddpclient

interface PongListener {
    fun onPong(responseMessage: Incoming.Pong)
}
