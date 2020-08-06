package com.nextbreakpoint.flink.common

data class SupervisorOptions(
    val clusterName: String,
    val pollingInterval: Long,
    val taskTimeout: Long,
    val dryRun: Boolean
)