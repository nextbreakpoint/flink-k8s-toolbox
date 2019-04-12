package com.nextbreakpoint.model

data class JobCancelConfig(
    val descriptor: ClusterDescriptor,
    val savepointPath: String = "file:///var/tmp/savepoints",
    val savepoint: Boolean = false,
    val jobId: String
)