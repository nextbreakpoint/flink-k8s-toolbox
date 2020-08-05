package com.nextbreakpoint.flinkoperator.common

data class SupervisorOptions(
    val clusterName: String,
    val pollingInterval: Long,
    val taskTimeout: Long
)