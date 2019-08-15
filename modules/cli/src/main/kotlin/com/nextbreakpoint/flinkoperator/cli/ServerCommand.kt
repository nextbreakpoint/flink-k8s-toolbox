package com.nextbreakpoint.flinkoperator.cli

interface ServerCommand<T> {
    fun run(args: T)
}

