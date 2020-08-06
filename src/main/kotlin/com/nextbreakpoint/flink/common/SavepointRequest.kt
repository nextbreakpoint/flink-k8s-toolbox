package com.nextbreakpoint.flink.common

data class SavepointRequest(
    val jobId: String,
    val triggerId: String
)
