package com.nextbreakpoint.flinkoperator.common.model

data class BootstrapOptions(
    val jarPath: String,
    val className: String,
    val parallelism: Int,
    val savepointPath: String?,
    val arguments: List<String>
)