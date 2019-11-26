package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

class InitialiseCluster : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Starting)
        Status.setTaskAttempts(context.flinkCluster, 0)

        updateBootstrap(context.flinkCluster)

        Status.appendTasks(context.flinkCluster,
            listOf(
                ClusterTask.CreateResources,
                ClusterTask.CreateBootstrapJob,
                ClusterTask.ClusterRunning
            )
        )

        val taskManagers = context.flinkCluster.spec?.taskManagers ?: 0
        val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
        Status.setTaskManagers(context.flinkCluster, taskManagers)
        Status.setTaskSlots(context.flinkCluster, taskSlots)
        Status.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)

        val savepointPath = context.flinkCluster.spec?.operator?.savepointPath
        Status.setSavepointPath(context.flinkCluster, savepointPath)

        val labelSelector = ClusterResource.makeLabelSelector(context.clusterId)
        Status.setLabelSelector(context.flinkCluster, labelSelector)

        return skip(context.flinkCluster, "Cluster initialized")
    }
}