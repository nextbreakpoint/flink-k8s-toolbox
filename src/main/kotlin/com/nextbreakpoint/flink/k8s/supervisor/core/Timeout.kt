package com.nextbreakpoint.flink.k8s.supervisor.core

object Timeout {
    val TASK_TIMEOUT = System.getenv("TASK_TIMEOUT")?.toLong() ?: 60L
}
