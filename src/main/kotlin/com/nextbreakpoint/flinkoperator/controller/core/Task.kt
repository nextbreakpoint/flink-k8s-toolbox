package com.nextbreakpoint.flinkoperator.controller.core

abstract class Task {
    abstract fun execute(context: TaskContext)
}