package com.sunya.netchdf.util

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.experimental.ExperimentalNativeApi

@OptIn(ExperimentalNativeApi::class)
actual fun useDefaultNThreads(): Int {
    return Platform.getAvailableProcessors() / 2
}

private const val debug = false

// TODO
actual class Deque<T> actual constructor(initialCapacity: Int) {
    private val delegate = ArrayDeque<T>(initialCapacity) // could be a queue ? or a stack with a limit
    private val mutex = Mutex()
    private var complete = false

    actual fun add(item: T) {
        if (debug) println("Deque add")
        runBlocking {
            mutex.withLock {
                if (debug) println(" Deque add lock")
                delegate.add(item)
            }
            if (debug) println(" Deque add lock done")
        }
        if (debug) println(" Deque add done")
    }

    actual fun next(): T? {
        if (debug) println("Deque next")

        var countWaits = 0
        while (countWaits < 1000) {
            var firstElement: T? = null
            runBlocking {
                if (debug) println(" Deque next withlock")
                mutex.withLock {
                    firstElement = delegate.removeFirstOrNull()
                }
            }
            if (firstElement != null) {
                if (debug)  println(" Deque got firstElement")
                return firstElement
            } else if (complete) {
                if (debug) println(" Deque complete")
                return null
            } else {
                if (debug) println(" Deque blocking")
                runBlocking {
                    delay(10)
                }
                countWaits++
                if (debug) println(" Deque countWaits $countWaits")
            }
        }
        throw RuntimeException("Deque next timed out (10 sec)")
    }

    actual fun complete() {
        println(" complete called")
        complete = true
    }
}