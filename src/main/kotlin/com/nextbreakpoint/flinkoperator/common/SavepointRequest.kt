package com.nextbreakpoint.flinkoperator.common

data class SavepointRequest(
    val jobId: String,
    val triggerId: String
)
