package com.nextbreakpoint.flinkoperator.testing

import org.mockito.Mockito
import org.mockito.Mockito.`when`
import org.mockito.stubbing.OngoingStubbing

object KotlinMockito {
    fun <T> any(): T {
        Mockito.any<T>()
        return uninitialized()
    }

    fun <T> eq(value : T): T {
        Mockito.eq(value)
        return value ?: uninitialized()
    }

    fun <T> given(value : T): OngoingStubbing<T> = `when`(value)

    @Suppress("UNCHECKED_CAST")
    private fun <T> uninitialized(): T = null as T
}