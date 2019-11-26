package com.nextbreakpoint.flinkoperator.controller.core

data class TaskResult<T>(val action: TaskAction, val output: T)
