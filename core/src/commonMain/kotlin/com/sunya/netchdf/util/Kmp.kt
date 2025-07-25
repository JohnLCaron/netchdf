package com.sunya.netchdf.util


expect fun useDefaultNThreads(): Int

// add to end, take from head
expect class Deque<T>(initialCapacity: Int) {
    fun add(item: T)
    fun next(): T?
    fun done()
}
