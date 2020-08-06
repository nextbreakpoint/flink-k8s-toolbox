package com.nextbreakpoint.flink.vertex.core

object Timeout {
    val POLLING_INTERVAL = System.getenv("POLLING_INTERVAL")?.toLong() ?: 5L
}
