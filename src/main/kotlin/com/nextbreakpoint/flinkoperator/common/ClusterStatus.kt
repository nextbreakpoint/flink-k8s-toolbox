package com.nextbreakpoint.flinkoperator.common

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
    Restarting,
    Cancelling
}