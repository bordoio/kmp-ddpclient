package io.bordo.ddpclient.ddpclient

interface MethodResultListener {
    suspend fun onResult(responseMessage: Incoming.Result)
    suspend fun onUpdated(responseMessage: Incoming.Updated)
    suspend fun onConnectionClosed(exception: Exception)
    suspend fun onTimeout()
    suspend fun onException(exception: Exception)
}
