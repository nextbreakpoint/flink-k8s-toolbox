package com.nextbreakpoint.model

data class SidecarConfig(
    val image: String,
    val pullSecrets: String,
    val pullPolicy: String?,
    val arguments: String?
)
