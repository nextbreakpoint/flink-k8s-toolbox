package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class CreateResources : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CREATING_CLUSTER_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val clusterScaling = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val changes = computeChanges(context.flinkCluster)

        val response = context.isClusterReady(context.clusterId, clusterScaling)

        if (changes.isEmpty() && response.isCompleted()) {
            return skip(context.flinkCluster, "Resources already created")
        }

        val resources = makeClusterResources(context.clusterId, context.flinkCluster)

        val createResourcesResponse = context.createClusterResources(context.clusterId, resources)

        if (!createResourcesResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Retry creating resources...")
        }

        updateDigests(context.flinkCluster)
        updateBootstrap(context.flinkCluster)

        return next(context.flinkCluster, "Creating resources...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.CREATING_CLUSTER_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val clusterScale = ClusterScaling(
            taskManagers = context.flinkCluster.status.taskManagers,
            taskSlots = context.flinkCluster.status.taskSlots
        )

        val response = context.isClusterReady(context.clusterId, clusterScale)

        if (!response.isCompleted()) {
            return repeat(context.flinkCluster, "Creating resources...")
        }

        return next(context.flinkCluster, "Resources created after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Resources created")
    }
}