package com.nextbreakpoint.model

data class JobSubmitConfig(
    val descriptor: ClusterDescriptor,
    val className: String,
    val jarPath: String,
    val arguments: String,
    val savepoint: String,
    val parallelism: Int
)