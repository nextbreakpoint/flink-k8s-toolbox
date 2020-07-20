package com.nextbreakpoint.flinkoperator.common.model

data class SavepointRequest(
    val jobId: String,
    val triggerId: String
)
