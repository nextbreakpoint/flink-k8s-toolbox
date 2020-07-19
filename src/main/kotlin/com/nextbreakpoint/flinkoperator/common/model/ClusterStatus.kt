package com.nextbreakpoint.flinkoperator.common.model

enum class ClusterStatus {
    Unknown,
    Starting,
    Stopping,
    Updating,
    Scaling,
    Running,
    Failed,
    Finished,
    Suspended,
    Terminated,
    Cancelling
}