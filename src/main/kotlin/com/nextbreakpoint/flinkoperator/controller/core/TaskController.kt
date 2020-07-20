package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.task.OnCancelling
import com.nextbreakpoint.flinkoperator.controller.task.OnFailed
import com.nextbreakpoint.flinkoperator.controller.task.OnFinished
import com.nextbreakpoint.flinkoperator.controller.task.OnInitialize
import com.nextbreakpoint.flinkoperator.controller.task.OnRunning
import com.nextbreakpoint.flinkoperator.controller.task.OnScaling
import com.nextbreakpoint.flinkoperator.controller.task.OnStarting
import com.nextbreakpoint.flinkoperator.controller.task.OnStopping
import com.nextbreakpoint.flinkoperator.controller.task.OnSuspended
import com.nextbreakpoint.flinkoperator.controller.task.OnTerminated
import com.nextbreakpoint.flinkoperator.controller.task.OnRestarting
import com.nextbreakpoint.flinkoperator.controller.task.OnUpdating
import org.apache.log4j.Logger

class TaskController(
    private val controller: OperationController,
    private val clusterSelector: ClusterSelector,
    private val logger: Logger,
    private val tasks: Map<ClusterStatus, Task>
) {
    companion object {
        fun create(controller: OperationController, clusterSelector: ClusterSelector): TaskController {
            val logger = Logger.getLogger("TaskController (" + clusterSelector.name + ")")

            val tasks = mapOf(
                ClusterStatus.Unknown to OnInitialize(),
                ClusterStatus.Starting to OnStarting(),
                ClusterStatus.Stopping to OnStopping(),
                ClusterStatus.Updating to OnUpdating(),
                ClusterStatus.Scaling to OnScaling(),
                ClusterStatus.Running to OnRunning(),
                ClusterStatus.Failed to OnFailed(),
                ClusterStatus.Finished to OnFinished(),
                ClusterStatus.Suspended to OnSuspended(),
                ClusterStatus.Terminated to OnTerminated(),
                ClusterStatus.Restarting to OnRestarting(),
                ClusterStatus.Cancelling to OnCancelling()
            )

            return TaskController(controller, clusterSelector, logger, tasks)
        }

        // required for testing
        fun create(controller: OperationController, clusterSelector: ClusterSelector, tasks: Map<ClusterStatus, Task>): TaskController {
            val logger = Logger.getLogger("TaskController (" + clusterSelector.name + ")")

            return TaskController(controller, clusterSelector, logger, tasks)
        }
    }

    fun execute(resources: CachedResources) {
        try {
            val cluster = resources.flinkCluster ?: throw RuntimeException("Cluster not present")

            logger.info("Resource version: ${cluster.metadata?.resourceVersion}")

            val mediator = TaskMediator(clusterSelector, cluster, resources, controller)

            val actionTimestamp = mediator.getActionTimestamp()

            val statusTimestamp = mediator.getStatusTimestamp()

            val hasFinalizer = mediator.hasFinalizer()

            val status = mediator.getClusterStatus()

            logger.info("Cluster status: $status")

            val context = TaskContext(logger, mediator)

            tasks[status]?.execute(context)

            mediator.refreshStatus(logger, statusTimestamp, actionTimestamp, hasFinalizer)
        } catch (e : Exception) {
            logger.error("Error occurred while reconciling resource ${clusterSelector.name}", e)
        }
    }
}
