package com.nextbreakpoint.flinkoperator.controller.core

object Timeout {
    val POLLING_INTERVAL = System.getenv("POLLING_INTERVAL")?.toLong() ?: 5L
    val TASK_TIMEOUT = System.getenv("TASK_TIMEOUT")?.toLong() ?: 300L
}
