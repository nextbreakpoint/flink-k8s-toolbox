package com.nextbreakpoint.flinkoperator.server.supervisor.core

object Timeout {
    val TASK_TIMEOUT = System.getenv("TASK_TIMEOUT")?.toLong() ?: 300L
}
