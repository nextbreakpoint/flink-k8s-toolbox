package com.nextbreakpoint.flinkoperator.common

data class BootstrapOptions(
    val clusterName: String,
    val jarPath: String,
    val className: String,
    val parallelism: Int,
    val savepointPath: String?,
    val arguments: List<String>
)