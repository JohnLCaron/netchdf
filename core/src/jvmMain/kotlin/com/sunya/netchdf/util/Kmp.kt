package com.sunya.netchdf.util

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.lang.Runtime.*
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.RuntimeException

// todo: i have 2 threads per processer, these dont help the IO i think.
actual fun useDefaultNThreads(): Int {
    return getRuntime().availableProcessors() / 2
}

private const val debug = false

actual class Deque<T> actual constructor(initialCapacity: Int) {
    val delegate = ConcurrentLinkedDeque<T>()
    var complete = false

    actual fun add(item: T) {
        if (debug) println(" Deque add")
        delegate.add(item)
    }

    actual fun next(): T? {

        var countWaits = 0
        while (countWaits < 1000) {
            val firstElement = delegate.pollFirst()
            if (firstElement != null) {
                if (debug) println(" Deque got element")
                return firstElement
            } else if (complete) {
                if (debug) println(" Deque complete")
                return null
            } else {
                if (debug) println(" Deque wait")
                runBlocking {
                    delay(10)
                }
                countWaits++
            }
        }
        throw RuntimeException("Deque next timed out (10 sec)")
    }

    actual fun complete() {
        if (debug) println(" Deque complete was called")
        complete = true
    }
}