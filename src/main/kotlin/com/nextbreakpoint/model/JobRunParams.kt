package com.nextbreakpoint.model

data class JobRunParams(
    val descriptor: ClusterDescriptor,
    val sidecar: SidecarConfig
)