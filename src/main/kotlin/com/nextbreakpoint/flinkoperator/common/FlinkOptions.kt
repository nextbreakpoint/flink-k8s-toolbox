package com.nextbreakpoint.flinkoperator.common

data class FlinkOptions(
    val hostname: String?,
    val portForward: Int?,
    val useNodePort: Boolean
)