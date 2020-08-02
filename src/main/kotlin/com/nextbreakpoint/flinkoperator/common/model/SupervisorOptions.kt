package com.nextbreakpoint.flinkoperator.common.model

data class SupervisorOptions(
    val clusterName: String,
    val pollingInterval: Long,
    val taskTimeout: Long
)