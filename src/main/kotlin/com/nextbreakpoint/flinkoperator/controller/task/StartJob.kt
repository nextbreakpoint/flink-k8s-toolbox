package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class StartJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STARTING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStartedResponse = context.controller.isJobStarted(context.clusterId)

        if (jobStartedResponse.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Job of cluster ${context.flinkCluster.metadata.name} already started")
        }

        val startJobResponse = context.controller.startJob(context.clusterId, context.flinkCluster)

        if (!startJobResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry starting job of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Starting job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STARTING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jobStartedResponse = context.controller.isJobStarted(context.clusterId)

        if (!jobStartedResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait for creation of job of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Job of cluster ${context.flinkCluster.metadata.name} started in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}