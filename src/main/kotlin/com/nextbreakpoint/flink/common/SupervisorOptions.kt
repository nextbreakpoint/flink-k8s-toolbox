package com.nextbreakpoint.flink.common

data class SupervisorOptions(
    val port: Int,
    val keystorePath: String?,
    val keystoreSecret: String?,
    val truststorePath: String?,
    val truststoreSecret: String?,
    val clusterName: String,
    val pollingInterval: Long,
    val taskTimeout: Long,
    val dryRun: Boolean
)