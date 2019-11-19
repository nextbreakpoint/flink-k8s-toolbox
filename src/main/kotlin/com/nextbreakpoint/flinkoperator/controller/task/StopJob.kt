package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class StopJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return taskFailedWithOutput(context.flinkCluster, "Cluster ${context.flinkCluster.metadata.name} doesn't have a job")
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.STOPPING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to stop job of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val response = context.controller.isJobStopped(context.clusterId)

        if (response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Job of cluster ${context.flinkCluster.metadata.name} already stopped")
        }

        val stopJobResponse = context.controller.stopJob(context.clusterId)

        if (stopJobResponse.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Stopping job of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Retry stopping job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return taskFailedWithOutput(context.flinkCluster, "Cluster ${context.flinkCluster.metadata.name} doesn't have a job")
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.STOPPING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to stop job of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val response = context.controller.isJobStopped(context.clusterId)

        if (response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Job of cluster ${context.flinkCluster.metadata.name} stopped in $seconds seconds")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Wait for termination of job of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}