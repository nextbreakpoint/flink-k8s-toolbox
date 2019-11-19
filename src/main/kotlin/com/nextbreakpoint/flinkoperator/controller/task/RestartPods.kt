package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class RestartPods : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to restart pods of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val clusterResources = createClusterResources(context.clusterId, context.flinkCluster)

        val response = context.controller.restartPods(context.clusterId, clusterResources)

        if (response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Restarting pods of cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Retry restarting pods of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to restart pods of cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.controller.isClusterReady(context.clusterId, clusterScaling)

        if (response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Resources of cluster ${context.flinkCluster.metadata.name} restarted in $seconds seconds")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Wait for creation of pods of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}