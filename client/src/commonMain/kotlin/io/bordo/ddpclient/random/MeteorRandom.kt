package io.bordo.ddpclient.random

import kotlin.math.floor
import kotlin.random.Random as KotlinRandom

/**
 * Kotlin port of Meteor's `random` package, used to reproduce Meteor's
 * "consistent ID generation" for Optimistic UI.
 *
 * When a Method is invoked with a `randomSeed`, the Meteor server seeds a
 * deterministic PRNG from it and draws document `_id`s from a stream scoped by
 * `"/collection/<name>"`. By replaying the exact same algorithm on the client we
 * can predict the `_id` the server will assign, before the Method round-trips.
 *
 * The Alea/Mash core below is a 1:1 port of Meteor's implementation
 * (http://baagoe.org/en/wiki/Better_random_numbers_for_javascript) and must stay
 * byte-identical to it — do not "clean up" the arithmetic.
 */
object MeteorRandom {

    /**
     * Build a deterministic generator from [seeds] — mirrors Meteor's
     * `Random.createWithSeeds(...)`. For a collection insert the seeds are
     * `[randomSeed, "/collection/<name>"]`.
     */
    fun createWithSeeds(vararg seeds: Any): RandomGenerator =
        AleaRandomGenerator(seeds.toList())

    /** Non-deterministic generator — mirrors Meteor's default insecure `Random`. */
    fun insecure(): RandomGenerator = InsecureRandomGenerator()

    /** Generate a fresh `randomSeed` to send alongside a DDP Method call. */
    fun newSeed(): String = insecure().hexString(20)

    /**
     * Predict the `_id` a Meteor server insert into [collectionName] will produce
     * for a Method invoked with [randomSeed]. Assumes the server generates the id
     * as the first draw from the `"/collection/<name>"`-scoped stream.
     */
    fun predictDocumentId(randomSeed: String, collectionName: String): String =
        createWithSeeds(randomSeed, "/collection/$collectionName").id()
}

private const val UNMISTAKABLE_CHARS =
    "23456789ABCDEFGHJKLMNPQRSTWXYZabcdefghijkmnopqrstuvwxyz"
private const val BASE64_CHARS =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
private const val TWO_POW_32 = 4294967296.0 // 2^32
private const val MASH_SEED = 0xefc8249dL

// Equivalent of JS `x >>> 0` (ToUint32). Intermediate values are never negative
// in this algorithm, so floor + mod 2^32 is sufficient.
private fun toUint32(x: Double): Double {
    val floored = floor(x)
    val mod = floored % TWO_POW_32
    return if (mod < 0) mod + TWO_POW_32 else mod
}

/**
 * Port of JS `Mash()`. Each instance carries its own `n` state; consecutive calls
 * accumulate state exactly like the JS closure.
 */
private class Mash {
    private var n = MASH_SEED.toDouble()

    operator fun invoke(data: Any): Double {
        for (ch in data.toString()) {
            n += ch.code.toDouble()
            var h = 0.02519603282416938 * n
            n = toUint32(h)
            h -= n
            h *= n
            n = toUint32(h)
            h -= n
            n += h * TWO_POW_32
        }
        return toUint32(n) * 2.3283064365386963e-10 // 2^-32
    }
}

/**
 * Alea PRNG — NOT cryptographically strong. 1:1 port of Meteor's implementation.
 */
private class Alea(seeds: List<Any>) {
    private var s0: Double
    private var s1: Double
    private var s2: Double
    private var c: Double = 1.0

    init {
        val actualSeeds = seeds.ifEmpty { listOf(KotlinRandom.nextLong()) }
        val mash = Mash()
        s0 = mash(" ")
        s1 = mash(" ")
        s2 = mash(" ")

        for (seed in actualSeeds) {
            s0 -= mash(seed)
            if (s0 < 0) s0 += 1.0
            s1 -= mash(seed)
            if (s1 < 0) s1 += 1.0
            s2 -= mash(seed)
            if (s2 < 0) s2 += 1.0
        }
    }

    fun random(): Double {
        val t = (2091639.0 * s0) + (c * 2.3283064365386963e-10)
        s0 = s1
        s1 = s2
        c = floor(t)
        s2 = t - c
        return s2
    }
}

/**
 * Port of Meteor's `RandomGenerator`. Subclasses only provide [fraction]; all the
 * helpers (id/hexString/secret/choice) live here.
 */
abstract class RandomGenerator {
    abstract fun fraction(): Double

    fun hexString(digits: Int): String = randomString(digits, "0123456789abcdef")

    fun randomString(charsCount: Int, alphabet: String): String {
        val sb = StringBuilder(charsCount)
        repeat(charsCount) { sb.append(choice(alphabet)) }
        return sb.toString()
    }

    fun id(charsCount: Int = 17): String = randomString(charsCount, UNMISTAKABLE_CHARS)

    fun secret(charsCount: Int = 43): String = randomString(charsCount, BASE64_CHARS)

    fun choice(alphabet: String): Char {
        val index = (fraction() * alphabet.length).toInt()
        return alphabet[index]
    }
}

/** Deterministic generator backed by [Alea]. */
private class AleaRandomGenerator(seeds: List<Any> = emptyList()) : RandomGenerator() {
    private val alea = Alea(seeds)
    override fun fraction(): Double = alea.random()
}

/** Non-deterministic generator backed by the multiplatform [kotlin.random.Random]. */
private class InsecureRandomGenerator : RandomGenerator() {
    override fun fraction(): Double = KotlinRandom.nextDouble()
}
