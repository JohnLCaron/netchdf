package com.sunya.netchdf.util

import java.lang.Runtime.*
import java.util.concurrent.ConcurrentLinkedDeque

// todo: i have 2 threads per processer, these dont help the IO i think.
actual fun useDefaultNThreads(): Int {
    return getRuntime().availableProcessors() / 2
}

actual class Deque<T> actual constructor(initialCapacity: Int) {
    val delegate = ConcurrentLinkedDeque<T>()
    var done = false

    actual fun add(item: T) {
        delegate.add(item)
    }

    //actual fun next(): T? {
    //    return delegate.poll() // can i block until available ??
    //}

    actual fun next(): T? {
        var countWaits = 0
        while (true) {
            val firstElement = delegate.poll()
            if (firstElement != null) {
                return firstElement
            } else if (done) {
                done()
                return null
            } else if (countWaits > 100) {
                println("takes too long (10 sec)")
                done()
                return null
            } else {
                // wait 100 msecs
                Thread.sleep(100) // java only
                countWaits++
            }
        }

    }

    actual fun done() {
        done = true
    }
}