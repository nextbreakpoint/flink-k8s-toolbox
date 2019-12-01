package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import io.kubernetes.client.models.V1StatefulSet
import org.apache.log4j.Logger

class TaskExecutor(
    private val controller: OperationController,
    private val taskHandlers: Map<ClusterTask, Task>
) {
    companion object {
        private val logger: Logger = Logger.getLogger(TaskExecutor::class.simpleName)
    }

    fun update(clusterId: ClusterId, flinkCluster: V1FlinkCluster, cacheResources: CachedResources) {
        try {
            val context = TaskContext(clusterId, flinkCluster, cacheResources, controller)

            return if (Status.hasCurrentTask(flinkCluster)) {
                updatedClusterStatus(clusterId, context)
            } else {
                prepareClusterStatus(clusterId, context)
            }
        } catch (e : Exception) {
            logger.error("Error occurred while updating cluster ${clusterId.name}", e)
        }
    }

    fun forget(clusterId: ClusterId) {
        try {
            controller.terminatePods(clusterId)

            val result = controller.arePodsTerminated(clusterId)

            if (result.isCompleted()) {
                controller.deleteClusterResources(clusterId)
            }
        } catch (e : Exception) {
            logger.error("Error occurred while forgetting cluster ${clusterId.name}", e)
        }
    }

    private fun prepareClusterStatus(
        clusterId: ClusterId,
        context: TaskContext
    ) {
        logger.debug("Initialising cluster ${clusterId.name}...")

        Status.appendTasks(context.flinkCluster, listOf(ClusterTask.InitialiseCluster))
        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Unknown)
        Status.setTaskAttempts(context.flinkCluster, 0)
        Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)

        controller.updateStatus(clusterId, context.flinkCluster)
    }

    private fun updatedClusterStatus(
        clusterId: ClusterId,
        context: TaskContext
    ) {
        logger.debug("Updating cluster ${clusterId.name}...")

        val operatorTimestamp = Status.getOperatorTimestamp(context.flinkCluster)

        val actionTimestamp = Annotations.getActionTimestamp(context.flinkCluster)

        val taskStatus = Status.getCurrentTaskStatus(context.flinkCluster)

        val currentTask = Status.getCurrentTask(context.flinkCluster)

        val taskHandler = getOperatorTaskOrThrow(currentTask)

        invokeTaskAndUpdateStatus(context, taskStatus, taskHandler)

        val statefulSet = context.resources.taskmanagerStatefulSets[context.clusterId]

        updateStatus(context.flinkCluster, statefulSet)

        val newOperatorTimestamp = Status.getOperatorTimestamp(context.flinkCluster)

        if (operatorTimestamp != newOperatorTimestamp) {
            controller.updateStatus(clusterId, context.flinkCluster)
        }

        val newActionTimestamp = Annotations.getActionTimestamp(context.flinkCluster)

        if (actionTimestamp != newActionTimestamp) {
            controller.updateAnnotations(clusterId, context.flinkCluster)
        }
    }

    private fun updateStatus(flinkCluster: V1FlinkCluster, statefulSet: V1StatefulSet?) {
        val taskManagers = statefulSet?.status?.readyReplicas ?: 0
        if (Status.getActiveTaskManagers(flinkCluster) != taskManagers) {
            Status.setActiveTaskManagers(flinkCluster, taskManagers)
        }
        val taskSlots = flinkCluster.status?.taskSlots ?: 1
        if (Status.getTotalTaskSlots(flinkCluster) != taskManagers * taskSlots) {
            Status.setTotalTaskSlots(flinkCluster, taskManagers * taskSlots)
        }
        val savepointMode = flinkCluster.spec?.operator?.savepointMode
        if (Status.getSavepointMode(flinkCluster) != savepointMode) {
            Status.setSavepointMode(flinkCluster, savepointMode)
        }
        val jobRestartPolicy = flinkCluster.spec?.operator?.jobRestartPolicy
        if (Status.getJobRestartPolicy(flinkCluster) != jobRestartPolicy) {
            Status.setJobRestartPolicy(flinkCluster, jobRestartPolicy)
        }
    }

    private fun invokeTaskAndUpdateStatus(
        context: TaskContext,
        taskStatus: TaskStatus,
        task: Task
    ) {
        when (taskStatus) {
            TaskStatus.Executing -> {
                val result = task.onExecuting(context)
                if (result.output.isNotBlank()) {
                    if (result.action == TaskAction.FAIL) {
                        logger.warn(result.output)
                    } else {
                        logger.info(result.output)
                    }
                }
                when (result.action) {
                    TaskAction.NEXT -> {
                        Status.setTaskStatus(context.flinkCluster, TaskStatus.Awaiting)
                    }
                    TaskAction.FAIL -> {
                        Status.setTaskStatus(context.flinkCluster, TaskStatus.Failed)
                    }
                    TaskAction.SKIP -> {
                        if (Status.getNextOperatorTask(context.flinkCluster) != null) {
                            Status.selectNextTask(context.flinkCluster)
                            Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                        } else {
                            Status.setTaskStatus(context.flinkCluster, TaskStatus.Idle)
                        }
                    }
                    else -> {}
                }
            }
            TaskStatus.Awaiting -> {
                val result = task.onAwaiting(context)
                if (result.output.isNotBlank()) {
                    if (result.action == TaskAction.FAIL) {
                        logger.warn(result.output)
                    } else {
                        logger.info(result.output)
                    }
                }
                when (result.action) {
                    TaskAction.NEXT -> {
                        Status.setTaskStatus(context.flinkCluster, TaskStatus.Idle)
                    }
                    TaskAction.FAIL -> {
                        Status.setTaskStatus(context.flinkCluster, TaskStatus.Failed)
                    }
                    TaskAction.SKIP -> {
                        if (Status.getNextOperatorTask(context.flinkCluster) != null) {
                            Status.selectNextTask(context.flinkCluster)
                            Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                        } else {
                            Status.setTaskStatus(context.flinkCluster, TaskStatus.Idle)
                        }
                    }
                    else -> {}
                }
            }
            TaskStatus.Idle -> {
                val result = task.onIdle(context)
                if (result.output.isNotBlank()) {
                    if (result.action == TaskAction.FAIL) {
                        logger.warn(result.output)
                    } else {
                        logger.info(result.output)
                    }
                }
                when (result.action) {
                    TaskAction.FAIL -> {
                        Status.setTaskStatus(context.flinkCluster, TaskStatus.Failed)
                    }
                    TaskAction.NEXT -> {
                        if (Status.getNextOperatorTask(context.flinkCluster) != null) {
                            Status.selectNextTask(context.flinkCluster)
                            Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                        }
                    }
                    else -> {}
                }
            }
            TaskStatus.Failed -> {
                val result = task.onFailed(context)
                if (result.output.isNotBlank()) {
                    logger.warn(result.output)
                }
                Status.setClusterStatus(context.flinkCluster, ClusterStatus.Failed)
                Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                Status.resetTasks(context.flinkCluster, listOf(ClusterTask.ClusterHalted))
            }
        }
    }

    private fun getOperatorTaskOrThrow(clusterTask: ClusterTask) =
        taskHandlers[clusterTask] ?: throw RuntimeException("Implementation not found for task $clusterTask")
}
