package com.nextbreakpoint.flink.common

enum class ClusterStatus {
    Unknown,
    Starting,
    Started,
    Stopping,
    Stopped,
    Terminated
}