package com.nextbreakpoint.flinkoperator.server.supervisor.core

abstract class Task {
    abstract fun execute(context: TaskContext)
}