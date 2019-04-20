package com.nextbreakpoint.model

data class JobSubmitParams(
    val descriptor: ClusterDescriptor,
    val jarPath: String,
    val className: String?,
    val arguments: String?,
    val savepoint: String?,
    val parallelism: Int = 1
)