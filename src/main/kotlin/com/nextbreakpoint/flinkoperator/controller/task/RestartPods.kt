package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class RestartPods : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val resources = makeClusterResources(context.clusterId, context.flinkCluster)

        val response = context.restartPods(context.clusterId, resources)

        if (!response.isCompleted()) {
            return repeat(context.flinkCluster, "Retry restarting pods...")
        }

        return next(context.flinkCluster, "Restarting pods...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TERMINATING_RESOURCES_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.isClusterReady(context.clusterId, clusterScaling)

        if (!response.isCompleted()) {
            return repeat(context.flinkCluster, "Restarting pods...")
        }

        return next(context.flinkCluster, "Resources restarted after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Pods restarted")
    }
}