package com.nextbreakpoint.flinkoperator.cli

interface LaunchCommand<T> {
    fun run(args: T)
}

