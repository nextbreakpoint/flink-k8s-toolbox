package com.nextbreakpoint.flinkoperator.server.supervisor

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.ClusterStatus
import com.nextbreakpoint.flinkoperator.server.controller.Controller
import com.nextbreakpoint.flinkoperator.server.supervisor.core.CachedResources
import com.nextbreakpoint.flinkoperator.server.supervisor.core.Task
import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskContext
import com.nextbreakpoint.flinkoperator.server.supervisor.core.TaskController
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnCancelling
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnFailed
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnFinished
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnInitialize
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnRunning
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnScaling
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnStarting
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnStopping
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnSuspended
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnTerminated
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnRestarting
import com.nextbreakpoint.flinkoperator.server.supervisor.task.OnUpdating
import org.apache.log4j.Logger

class Supervisor(
    private val controller: Controller,
    private val logger: Logger,
    private val tasks: Map<ClusterStatus, Task>
) {
    companion object {
        fun create(controller: Controller, loggerName: String): Supervisor {
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
        fun create(controller: Controller, tasks: Map<ClusterStatus, Task>, loggerName: String): Supervisor {
            val logger = Logger.getLogger(loggerName)
            return Supervisor(controller, logger, tasks)
        }
    }

    fun reconcile(clusterSelector: ClusterSelector, resources: CachedResources) {
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
    }
}
