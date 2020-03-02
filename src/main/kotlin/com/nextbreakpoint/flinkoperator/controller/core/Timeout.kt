package com.nextbreakpoint.flinkoperator.controller.core

object Timeout {
    val POLLING_INTERVAL = System.getenv("POLLING_INTERVAL").toLongOrNull() ?: 5L
    val TASK_TIMEOUT = System.getenv("TASK_TIMEOUT").toLongOrNull() ?: 300L
}
