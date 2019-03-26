package com.nextbreakpoint.model

data class JobCancelConfig(
    val descriptor: ClusterDescriptor,
    val savepoint: Boolean = false,
    val jobId: String
)