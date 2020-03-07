package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.task.OnCancelling
import com.nextbreakpoint.flinkoperator.controller.task.OnFailed
import com.nextbreakpoint.flinkoperator.controller.task.OnInitialize
import com.nextbreakpoint.flinkoperator.controller.task.OnRunning
import com.nextbreakpoint.flinkoperator.controller.task.OnScaling
import com.nextbreakpoint.flinkoperator.controller.task.OnStarting
import com.nextbreakpoint.flinkoperator.controller.task.OnStopping
import com.nextbreakpoint.flinkoperator.controller.task.OnSuspended
import com.nextbreakpoint.flinkoperator.controller.task.OnTerminated
import com.nextbreakpoint.flinkoperator.controller.task.OnUpdating
import org.apache.log4j.Logger
import java.lang.RuntimeException

class TaskController(private val controller: OperationController, private val clusterId: ClusterId) {
    private val logger = Logger.getLogger("TaskController (" + clusterId.name + ")")

    private val tasks = mapOf(
        ClusterStatus.Unknown to OnInitialize(logger),
        ClusterStatus.Starting to OnStarting(logger),
        ClusterStatus.Stopping to OnStopping(logger),
        ClusterStatus.Updating to OnUpdating(logger),
        ClusterStatus.Scaling to OnScaling(logger),
        ClusterStatus.Running to OnRunning(logger),
        ClusterStatus.Failed to OnFailed(logger),
        ClusterStatus.Suspended to OnSuspended(logger),
        ClusterStatus.Terminated to OnTerminated(logger),
        ClusterStatus.Cancelling to OnCancelling(logger)
    )

    fun execute(resources: CachedResources) {
        try {
            val cluster = resources.flinkCluter ?: throw RuntimeException("Cluster not present")

            logger.info("Resource version: ${cluster.metadata?.resourceVersion}")

            val context = TaskContext(clusterId, cluster, resources, controller)

            val actionTimestamp = context.getActionTimestamp()

            val statusTimestamp = context.getStatusTimestamp()

            val hasFinalizer = context.hasFinalizer()

            val status = context.getClusterStatus()

            logger.info("Cluster status: $status")

            tasks[status]?.execute(context)

            context.refreshStatus(statusTimestamp, actionTimestamp, hasFinalizer)
        } catch (e : Exception) {
            logger.error("Error occurred while reconciling resource ${clusterId.name}", e)
        }
    }
}
