package io.bordo.ddpclient.util

import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.okhttp.OkHttp

/**
 * Created by Osman Saral on 13.04.2023
 */
actual val PlatformEngine: HttpClientEngine = OkHttp.create { }
