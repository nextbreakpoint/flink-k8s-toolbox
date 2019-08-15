package com.nextbreakpoint.flinkoperator.common.model

data class FlinkOptions(
    val hostname: String?,
    val portForward: Int?,
    val useNodePort: Boolean
)