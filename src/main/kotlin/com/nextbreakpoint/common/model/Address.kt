package com.nextbreakpoint.common.model

data class Address(
    val host: String,
    val port: Int,
    val keystorePath: String,
    val keystoreSecret: String?
)