package com.nextbreakpoint.common

interface ServerCommand<T> {
    fun run(config: T)
}

