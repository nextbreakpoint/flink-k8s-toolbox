package com.nextbreakpoint.model

data class GetTaskManagerConfig(
    val descriptor: ClusterDescriptor,
    val taskmanagerId: String
)