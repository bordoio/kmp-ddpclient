package io.bordo.ddpclient.util

// iOS
import platform.Foundation.NSUUID

/**
 * Created by Osman Saral on 6.04.2023
 */
actual fun randomUUID(): String = NSUUID().UUIDString()
