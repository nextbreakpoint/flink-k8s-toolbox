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

class Supervisor(
    private val controller: OperationController,
    private val logger: Logger,
    private val tasks: Map<ClusterStatus, Task>
) {
    companion object {
        fun create(controller: OperationController, loggerName: String): Supervisor {
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

            return create(controller, tasks, loggerName)
        }

        // required for testing
        fun create(controller: OperationController, tasks: Map<ClusterStatus, Task>, loggerName: String): Supervisor {
            val logger = Logger.getLogger(loggerName)

            return Supervisor(controller, logger, tasks)
        }
    }

    fun reconcile(clusterSelector: ClusterSelector, resources: SupervisorCachedResources) {
        try {
            val cluster = resources.flinkCluster ?: throw RuntimeException("Cluster not present")

            logger.info("Resource version: ${cluster.metadata?.resourceVersion}")

            val mediator = TaskController(clusterSelector, cluster, resources, controller)

            val actionTimestamp = mediator.getActionTimestamp()

            val statusTimestamp = mediator.getStatusTimestamp()

            val hasFinalizer = mediator.hasFinalizer()

            val status = mediator.getClusterStatus()

            logger.info("Cluster status: $status")

            val context = TaskContext(logger, mediator)

            tasks[status]?.execute(context)

            mediator.refreshStatus(logger, statusTimestamp, actionTimestamp, hasFinalizer)
        } catch (e : Exception) {
            logger.error("Error occurred while reconciling cluster ${clusterSelector.name}", e)
        }
    }
}
