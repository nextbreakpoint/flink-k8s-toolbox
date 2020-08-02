package com.nextbreakpoint.flinkoperator.common.model

data class OperatorOptions(
    val port: Int,
    val keystorePath: String?,
    val keystoreSecret: String?,
    val truststorePath: String?,
    val truststoreSecret: String?
)