package com.nextbreakpoint.model

data class ControllerConfig(
    val port: Int,
    val portForward: Int?,
    val kubeConfig: String?
)
