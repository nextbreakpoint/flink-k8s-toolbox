package com.nextbreakpoint.model

data class ServerConfig(
    val port: Int,
    val portForward: Int?,
    val kubeConfig: String?
)
