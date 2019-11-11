package com.nextbreakpoint.flinkoperator.common.model

enum class ClusterStatus {
    Unknown,
    Starting,
    Stopping,
    Running,
    Failed,
    Suspended,
    Terminated,
    Checkpointing
}