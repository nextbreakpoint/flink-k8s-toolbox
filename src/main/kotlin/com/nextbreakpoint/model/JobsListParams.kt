package com.nextbreakpoint.model

data class JobsListParams(
    val descriptor: ClusterDescriptor,
    val savepoint: Boolean = false,
    val running: Boolean = false
)