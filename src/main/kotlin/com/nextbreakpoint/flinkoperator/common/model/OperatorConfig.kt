package com.nextbreakpoint.flinkoperator.common.model

data class OperatorConfig(
    val port: Int,
    val namespace: String,
    val flinkHostname: String?,
    val portForward: Int?,
    val useNodePort: Boolean,
    val keystorePath: String?,
    val keystoreSecret: String?,
    val truststorePath: String?,
    val truststoreSecret: String?
)