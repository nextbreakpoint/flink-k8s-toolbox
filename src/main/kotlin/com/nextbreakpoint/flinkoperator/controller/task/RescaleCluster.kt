package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import org.apache.log4j.Logger

class RescaleCluster : Task {
    companion object {
        private val logger = Logger.getLogger(RescaleCluster::class.simpleName)
    }

    override fun onExecuting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.RESCALING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to rescale task managers of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val desiredTaskManagers = Status.getTaskManagers(context.flinkCluster)

        val result = context.controller.setTaskManagersReplicas(context.clusterId, desiredTaskManagers)

        if (!result.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Can't rescale task managers of cluster ${context.clusterId.name}")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Task managers of cluster ${context.clusterId.name} have been rescaled")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.RESCALING_CLUSTER_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to scale task managers of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val result = context.controller.getTaskManagersReplicas(context.clusterId)

        if (!result.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Task managers of cluster ${context.clusterId.name} have not been scaled yet...")
        }

        val desiredTaskManagers = Status.getTaskManagers(context.flinkCluster)

        if (desiredTaskManagers != result.output) {
            return taskAwaitingWithOutput(context.flinkCluster, "Task managers of cluster ${context.clusterId.name} have not been scaled yet...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Task managers of cluster ${context.clusterId.name} have been scaled")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}