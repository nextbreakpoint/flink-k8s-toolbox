package com.nextbreakpoint.flink.common

data class RunnerOptions(
    val pollingInterval: Long,
    val taskTimeout: Long
)