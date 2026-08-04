package io.bordo.ddpclient.ddpclient

interface SubscriptionResultListener {
    suspend fun onReady(subId: String)
    suspend fun onNoSub(responseMessage: Incoming.NoSub)
}

abstract class UnSubscriptionResultListener() : SubscriptionResultListener {
    override suspend fun onReady(subId: String) {}
}
