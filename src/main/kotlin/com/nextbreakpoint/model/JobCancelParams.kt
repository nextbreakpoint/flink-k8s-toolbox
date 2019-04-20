package com.nextbreakpoint.model

data class JobCancelParams(
    val descriptor: ClusterDescriptor,
    val savepointPath: String = "file:///var/tmp/savepoints",
    val savepoint: Boolean = false,
    val jobId: String
)