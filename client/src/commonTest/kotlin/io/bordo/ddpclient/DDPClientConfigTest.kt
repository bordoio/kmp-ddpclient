package io.bordo.ddpclient

import io.bordo.ddpclient.ddpclient.DDPClientConfig
import io.bordo.ddpclient.ddpclient.ResponseError
import io.bordo.ddpclient.ddpclient.Utils
import io.ktor.http.URLProtocol
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Pins the public defaults of [DDPClientConfig]. For a library these are API: changing one changes
 * behaviour for every consumer that never touched the setting.
 */
class DDPClientConfigTest {

    @Test
    fun `connection defaults`() {
        val config = DDPClientConfig()

        assertEquals(URLProtocol.WSS, config.protocol)
        assertEquals(3, config.maxReconnectAttempts)
        assertEquals(500L, config.retryDelay)
        assertTrue(config.retryConnection)
        assertEquals(20_000L, config.timeout)
        assertEquals("1", config.ddpVersion)
    }

    @Test
    fun `ping pong defaults`() {
        val config = DDPClientConfig()

        assertTrue(config.pingPongEnabled)
        assertEquals(30_000L, config.pingPongInterval)
        assertEquals(15_000L, config.pingPongTimeout)
        assertTrue(
            config.pingPongTimeout < config.pingPongInterval,
            "a keepalive that times out later than it fires would never detect a dead peer",
        )
    }

    @Test
    fun `default socket path is a fresh sockjs path per config`() {
        assertTrue(DDPClientConfig().socketPath!!.matches(Regex("""sockjs/\d{3}/[A-Za-z0-9]{8}/websocket""")))
        assertTrue(DDPClientConfig().socketPath != DDPClientConfig().socketPath)
    }

    @Test
    fun `auth hooks default to no-ops so the client needs no auth wiring`() = runTest {
        val config = DDPClientConfig()

        // unauthorizedChecker defaulting to false is what keeps a plain client from trying to
        // re-authenticate on ordinary method errors.
        assertFalse(
            config.unauthorizedChecker("m", null, ResponseError("e", "r", "m", "t")),
        )
        assertTrue(config.tokenRefresher())
    }

    @Test
    fun `defaultJson is lenient about server drift`() {
        val configuration = io.bordo.ddpclient.ddpclient.defaultJson.configuration

        // These four are what stop a server-side schema change from breaking every consumer.
        assertTrue(configuration.ignoreUnknownKeys)
        assertTrue(configuration.coerceInputValues)
        assertTrue(configuration.isLenient)
        assertFalse(configuration.explicitNulls)
    }

    @Test
    fun `generateSocketPath produces a distinct path each call`() {
        val paths = List(50) { Utils.generateSocketPath() }

        paths.forEach {
            assertTrue(it.startsWith("sockjs/"), it)
            assertTrue(it.endsWith("/websocket"), it)
            val serverId = it.split("/")[1].toInt()
            assertTrue(serverId in 100..999, "server id out of SockJS range: $serverId")
            assertEquals(8, it.split("/")[2].length)
        }

        // Collisions would make two clients share a SockJS session; 50 draws from 900 * 62^8
        // colliding means the generator is broken, not unlucky.
        assertEquals(paths.size, paths.toSet().size)
    }
}
