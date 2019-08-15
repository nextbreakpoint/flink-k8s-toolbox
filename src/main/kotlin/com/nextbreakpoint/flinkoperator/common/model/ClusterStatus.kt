package com.nextbreakpoint.flinkoperator.common.model

enum class ClusterStatus {
    UNKNOWN,
    STARTING,
    STOPPING,
    RUNNING,
    FAILED,
    SUSPENDED,
    TERMINATED,
    CHECKPOINTING
}