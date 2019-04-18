package com.nextbreakpoint.model

data class RunJobConfig(
    val descriptor: ClusterDescriptor,
    val sidecar: SidecarConfig
)