package com.nextbreakpoint.flink.common

data class FlinkOptions(
    val hostname: String?,
    val portForward: Int?,
    val useNodePort: Boolean
)