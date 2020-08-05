package com.nextbreakpoint.flinkoperator.common

class JobStats(
    val totalNumberOfCheckpoints: Int,
    val numberOfCompletedCheckpoints: Int,
    val numberOfInProgressCheckpoints: Int,
    val numberOfFailedCheckpoints: Int,
    val lastCheckpointDuration: Long,
    val lastCheckpointSize: Long,
    val lastCheckpointRestoreTimestamp: Long,
    val lastCheckpointAlignmentBuffered: Long,
    val lastCheckpointExternalPath: String,
    val fullRestarts: Int,
    val restartingTime: Long,
    val uptime: Long,
    val downtime: Long
)
