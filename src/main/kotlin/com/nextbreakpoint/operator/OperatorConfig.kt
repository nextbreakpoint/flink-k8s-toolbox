package com.nextbreakpoint.operator

data class OperatorConfig(
    val port: Int,
    val namespace: String,
    val flinkHostname: String?,
    val portForward: Int?,
    val useNodePort: Boolean,
    val savepointInterval: Int,
    val keystorePath: String,
    val keystoreSecret: String?,
    val truststorePath: String,
    val truststoreSecret: String?
)