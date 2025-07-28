package com.sunya.netchdf.util

import com.sunya.cdm.api.ArraySection
import kotlinx.coroutines.runBlocking
import kotlin.experimental.ExperimentalNativeApi

@OptIn(ExperimentalNativeApi::class)
actual fun useDefaultNThreads(): Int {
    return Platform.getAvailableProcessors() / 2
}

// TODO
actual class Deque<T> actual constructor(initialCapacity: Int) {
    private val delegate = ArrayDeque<ArraySection<*>>(initialCapacity) // could be a queue ? or a stack with a limit
    //private val mutex = Mutex()

    actual fun add(item: T) = runBlocking {
        /* mutex.withLock {
            delegate.add(item)
        } */
    }

    actual fun next(): T? = runBlocking {
        /* mutex.withLock {
            return delegate.removeFirstOrNull()
        }
        return delegate.poll() // can i block until available ??
         */
        null
    }

    actual fun done() {

    }
}