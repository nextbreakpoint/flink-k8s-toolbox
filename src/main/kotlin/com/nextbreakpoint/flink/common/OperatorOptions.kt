package com.nextbreakpoint.flink.common

data class OperatorOptions(
    val port: Int,
    val keystorePath: String?,
    val keystoreSecret: String?,
    val truststorePath: String?,
    val truststoreSecret: String?,
    val pollingInterval: Long,
    val taskTimeout: Long,
    val dryRun: Boolean
)