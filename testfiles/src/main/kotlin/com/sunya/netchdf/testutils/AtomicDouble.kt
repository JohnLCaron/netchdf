package com.sunya.netchdf.testutils

import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

// doesnt work, dunno why ??
@OptIn(ExperimentalAtomicApi::class)
class AtomicDoubleNotWorking(initialValue: Double) {
    private val atomicReference = AtomicReference(initialValue)

    fun get(): Double = atomicReference.load()

    fun set(newValue: Double) {
        atomicReference.store(newValue)
    }

    fun getAndAdd(delta: Double): Double {
        var ntries = 0
        while (true) {
            val currentVal = atomicReference.load()
            val newValue = currentVal + delta
            if (atomicReference.compareAndSet(currentVal, newValue)) {
                return newValue
            }
            ntries++
            if (ntries > 100)
                println(ntries)
        }
    }
}

// AtomicLong with Bit Conversions: You can store the double or float value as its bit representation
// (e.g., using Double.doubleToLongBits() and Double.longBitsToDouble()) within an AtomicLong
// and perform atomic operations on the long value.
@OptIn(ExperimentalAtomicApi::class)
class AtomicDouble(initialValue: Double) {
    private val atomicReference = AtomicLong(initialValue.toBits())

    fun get(): Double = Double.fromBits(atomicReference.load())

    fun set(newValue: Double) {
        atomicReference.store(newValue.toBits())
    }

    fun getAndAdd(delta: Double): Double {
        while (true) {
            val currentVal = atomicReference.load()
            val newDouble = Double.fromBits(currentVal) + delta
            val newValue = newDouble.toBits()
            if (atomicReference.compareAndSet(currentVal, newValue)) {
                return newDouble
            }
        }
    }
}

// not quite ready WHY NOT ?
/*
import kotlinx.atomicfu.*

class AtomicDouble(initialValue: Double) {
    private val _value: AtomicRef<Double> = atomic(initialValue)

    fun get(): Double = _value.value

    fun set(newValue: Double) {
        _value.value = newValue
    }

    fun getAndSet(newValue: Double): Double = _value.getAndSet(newValue)

    fun compareAndSet(expect: Double, update: Double): Boolean = _value.compareAndSet(expect, update)

   // fun addAndGet(delta: Double): Double = _value.getAndAdd(delta) + delta

    fun getAndAdd(delta: Double): Double = _value.getAndAdd(delta)
} */