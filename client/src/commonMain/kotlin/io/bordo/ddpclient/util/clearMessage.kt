package io.bordo.ddpclient.util

fun clearMessage(message: String): String {
    if (message.isEmpty()) return message

    val len = message.length
    val (start, end) = when {
        len >= 5 &&
            message[0] == 'a' &&
            message[1] == '[' &&
            message[2] == '"' &&
            message[len - 2] == '"' &&
            message[len - 1] == ']' -> 3 to (len - 2)
        len >= 3 &&
            message[0] == 'a' &&
            message[1] == '[' &&
            message[len - 1] == ']' -> 2 to (len - 1)
        else -> return message
    }

    if (start >= end) return ""

    var firstSlash = -1
    for (i in start until end) {
        if (message[i] == '\\') {
            firstSlash = i
            break
        }
    }

    if (firstSlash < 0) return message.substring(start, end)

    val out = StringBuilder(end - start)
    out.append(message, start, firstSlash)

    var i = firstSlash
    while (i < end) {
        val ch = message[i]
        if (ch == '\\' && i + 1 < end) {
            when (val next = message[i + 1]) {
                '"', '\\' -> {
                    out.append(next)
                    i += 2
                    continue
                }
            }
        }
        out.append(ch)
        i++
    }

    return out.toString()
}
