package com.nextbreakpoint.model

data class JobScaleParams(
    val descriptor: ClusterDescriptor,
    val parallelism: Int,
    val jobId: String
)