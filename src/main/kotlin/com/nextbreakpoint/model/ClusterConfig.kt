package com.nextbreakpoint.model

data class ClusterConfig(
    val descriptor: ClusterDescriptor,
    val jobmanager: JobManagerConfig,
    val taskmanager: TaskManagerConfig
)