package com.nextbreakpoint.common.model

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