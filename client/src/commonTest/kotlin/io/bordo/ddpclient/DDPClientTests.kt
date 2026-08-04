package io.bordo.ddpclient

import app.cash.turbine.test
import io.bordo.ddpclient.db.memory.InMemoryDatabase
import io.bordo.ddpclient.ddpclient.DDPClient
import io.bordo.ddpclient.ddpclient.DDPClientConfig
import io.bordo.ddpclient.ddpclient.Incoming
import io.bordo.ddpclient.ddpclient.defaultJson
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respondBadRequest
import io.ktor.http.URLProtocol
import io.ktor.test.dispatcher.testSuspend
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Created by Osman Saral on 13.04.2023
 */
class DDPClientTests {

    @Test
    fun testJson() {
        val client = DDPClient("") {
            json = Json {
                ignoreUnknownKeys = true
                isLenient = false
                useArrayPolymorphism = true
            }
        }

        assertTrue(client.json.configuration.ignoreUnknownKeys)
        assertFalse(client.json.configuration.isLenient)
        assertTrue(client.json.configuration.useArrayPolymorphism)
    }

    @Test
    fun testDb() {
        val db = InMemoryDatabase(defaultJson)
        val client = DDPClient("") {
            this.db = db
        }
        assertSame(db, client.db)
    }

    @Test
    fun testHttpClient() {
        val client = HttpClient()
        val ddpClient = DDPClient("", clientFactory = {
            client
        })
        assertEquals(ddpClient.httpClient.engine, client.engine)
    }

    @Test
    fun testSocketPath() {
        val path = "path"
        val client = DDPClient("") {
            socketPath = path
        }
        assertEquals(path, client.socketPath)
    }

    @Test
    fun configTest() {
        val block: DDPClientConfig.() -> Unit = {
            protocol = URLProtocol.SOCKS
            println(protocol)
        }
        val config = DDPClientConfig().apply(block)

        assertEquals(URLProtocol.SOCKS, config.protocol)
    }

    @Test
    fun testProtocol() = testSuspend {
        var expectedProtocol: URLProtocol? = null

        val ddpClient = DDPClient("test", clientFactory = {
            HttpClient(MockEngine) {
                engine {
                    addHandler { request ->
                        expectedProtocol = request.url.protocol

                        respondBadRequest()
                    }
                }
            }
        }) {
            protocol = URLProtocol.SOCKS
        }

        try {
            runTest {
                ddpClient.initConnection().test {
                    awaitItem()
                    assertEquals(URLProtocol.SOCKS, expectedProtocol)
                }
            }
        } catch (a: AssertionError) {
            throw a
        } catch (_: Throwable) {
        }
    }

    @Test
    fun testHost() = testSuspend {
        var expectedHost: String? = null

        val ddpClient = DDPClient("test", clientFactory = {
            HttpClient(MockEngine) {
                engine {
                    addHandler { request ->
                        expectedHost = request.url.host

                        respondBadRequest()
                    }
                }
            }
        }) {
            retryDelay = 10
        }

        try {
            runTest {
                ddpClient.initConnection().test {
                    // initConnection() degrades every failure to Incoming.Exception rather than
                    // rethrowing, so the handshake failure arrives as an item, not an error.
                    assertIs<Incoming.Exception>(awaitItem())
                    assertEquals("test", expectedHost)
                }
            }
        } catch (a: AssertionError) {
            throw a
        } catch (_: Throwable) {
        }
    }

    @Test
    fun testChangingHost() = testSuspend {
        var expectedHost: String? = null

        val ddpClient = DDPClient("test", clientFactory = {
            HttpClient(MockEngine) {
                engine {
                    addHandler { request ->
                        expectedHost = request.url.host

                        respondBadRequest()
                    }
                }
            }
        }) {
            retryDelay = 10
        }

        try {
            runTest {
                ddpClient.host = "test1"
                ddpClient.initConnection().test {
                    assertIs<Incoming.Exception>(awaitItem())
                    assertEquals("test1", expectedHost)
                }
            }
        } catch (a: AssertionError) {
            throw a
        } catch (_: Throwable) {
        }
    }

    @Test
    fun testPath() = testSuspend {
        var expectedPath: String? = null

        val ddpClient = DDPClient("test", clientFactory = {
            HttpClient(MockEngine) {
                engine {
                    addHandler { request ->
                        expectedPath = request.url.encodedPath

                        respondBadRequest()
                    }
                }
            }
        })

        try {
            runTest {
                ddpClient.initConnection().test {
                    awaitItem()
                    assertEquals("/${ddpClient.socketPath}", expectedPath)
                }
            }
        } catch (a: AssertionError) {
            throw a
        } catch (_: Throwable) {
        }
    }

    @Test
    fun testNullSocketPath() = testSuspend {
        var expectedPath: String? = null

        val ddpClient = DDPClient("test", clientFactory = {
            HttpClient(MockEngine) {
                engine {
                    addHandler { request ->
                        expectedPath = request.url.encodedPath

                        respondBadRequest()
                    }
                }
            }
        }) {
            socketPath = null
        }

        try {
            runTest {
                ddpClient.initConnection().test {
                    awaitItem()
                    assertNull(ddpClient.socketPath)
                    assertEquals(0, expectedPath?.length)
                }
            }
        } catch (a: AssertionError) {
            throw a
        } catch (_: Throwable) {
        }
    }
}
