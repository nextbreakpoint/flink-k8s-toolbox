package com.nextbreakpoint.common.model

data class SavepointRequest(
    val jobId: String,
    val triggerId: String
)