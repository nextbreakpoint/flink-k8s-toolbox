package com.nextbreakpoint.model

data class JobListConfig(
    val descriptor: ClusterDescriptor,
    val savepoint: Boolean = false,
    val running: Boolean = false
)