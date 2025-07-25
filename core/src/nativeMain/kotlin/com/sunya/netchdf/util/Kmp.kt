package com.sunya.netchdf.util

actual fun getAvailableProcessors(): Int {
    return Platform.availableProcessors() / 2
}

// TODO
actual class Deque<T> actual constructor(initialCapacity: Int) {
    private val delegate = ArrayDeque<ArraySection<*>>(initialCapacity) // could be a queue ? or a stack with a limit
    private val mutex = Mutex()

    actual fun add(item: T) = runBlocking {
        mutex.withLock {
            delegate.add(item)
        }
    }

    actual fun next(): T? = runBlocking {
        mutex.withLock {
            return deque.removeFirstOrNull()
        }
        return delegate.poll() // can i block until available ??
    }
}