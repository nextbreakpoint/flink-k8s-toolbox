package com.nextbreakpoint.flink.common

data class BootstrapOptions(
    val clusterName: String,
    val jobName: String,
    val jarPath: String,
    val className: String,
    val parallelism: Int,
    val savepointPath: String?,
    val arguments: List<String>,
    val dryRun: Boolean
)