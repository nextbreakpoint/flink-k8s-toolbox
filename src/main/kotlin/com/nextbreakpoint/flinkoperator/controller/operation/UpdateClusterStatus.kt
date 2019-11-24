package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import io.kubernetes.client.models.V1StatefulSet
import org.apache.log4j.Logger

class UpdateClusterStatus(
    private val controller: OperationController
) : Operation<Void?, Void?>(
    controller.flinkOptions,
    controller.flinkClient,
    controller.kubeClient
) {
    companion object {
        private val logger: Logger = Logger.getLogger(UpdateClusterStatus::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val flinkCluster = controller.cache.getFlinkCluster(clusterId)

            val resources = controller.cache.getResources()

            val context = TaskContext(
                clusterId, flinkCluster, resources, controller
            )

            return if (Status.hasCurrentTask(flinkCluster)) {
                updatedClusterStatus(clusterId, context)
            } else {
                prepareClusterStatus(clusterId, context)
            }
        } catch (e : Exception) {
            logger.error("Error occurred while updating cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }

    private fun prepareClusterStatus(
        clusterId: ClusterId,
        context: TaskContext
    ): Result<Void?> {
        logger.debug("Initialising cluster ${clusterId.name}...")

        Status.appendTasks(context.flinkCluster, listOf(ClusterTask.InitialiseCluster))
        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Unknown)
        Status.setTaskAttempts(context.flinkCluster, 0)
        Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)

        controller.updateStatus(clusterId, context.flinkCluster)

        return Result(
            ResultStatus.SUCCESS,
            null
        )
    }

    private fun updatedClusterStatus(
        clusterId: ClusterId,
        context: TaskContext
    ): Result<Void?> {
        logger.debug("Updating cluster ${clusterId.name}...")

        val operatorTimestamp = Status.getOperatorTimestamp(context.flinkCluster)

        val actionTimestamp = Annotations.getActionTimestamp(context.flinkCluster)

        val taskStatus = Status.getCurrentTaskStatus(context.flinkCluster)

        val currentTask = Status.getCurrentTask(context.flinkCluster)

        val taskHandler = getOperatorTaskOrThrow(currentTask)

        val taskResult = invokeTaskAndUpdateStatus(context, taskStatus, taskHandler)

        val statefulSet = context.resources.taskmanagerStatefulSets[context.clusterId]

        updateStatusTaskManagers(context.flinkCluster, statefulSet)

        val newOperatorTimestamp = Status.getOperatorTimestamp(context.flinkCluster)

        if (operatorTimestamp != newOperatorTimestamp) {
            controller.updateStatus(clusterId, context.flinkCluster)
        }

        val newActionTimestamp = Annotations.getActionTimestamp(context.flinkCluster)

        if (actionTimestamp != newActionTimestamp) {
            controller.updateAnnotations(clusterId, context.flinkCluster)
        }

        return when {
            taskResult.status == ResultStatus.SUCCESS -> {
                if (taskResult.output?.isNotBlank() == true) logger.info(taskResult.output)

                Result(
                    ResultStatus.SUCCESS,
                    null
                )
            }
            taskResult.status == ResultStatus.AWAIT -> {
                if (taskResult.output?.isNotBlank() == true) logger.info(taskResult.output)

                Result(
                    ResultStatus.AWAIT,
                    null
                )
            }
            else -> {
                if (taskResult.output?.isNotBlank() == true) logger.warn(taskResult.output)

                Result(
                    ResultStatus.FAILED,
                    null
                )
            }
        }
    }

    private fun updateStatusTaskManagers(flinkCluster: V1FlinkCluster, statefulSet: V1StatefulSet?) {
        val taskManagers = statefulSet?.status?.readyReplicas ?: 0
        if (Status.getActiveTaskManagers(flinkCluster) != taskManagers) {
            Status.setActiveTaskManagers(flinkCluster, taskManagers)
        }
        val taskSlots = flinkCluster.status?.taskSlots ?: 1
        if (Status.getTotalTaskSlots(flinkCluster) != taskManagers * taskSlots) {
            Status.setTotalTaskSlots(flinkCluster,taskManagers * taskSlots)
        }
    }

    private fun invokeTaskAndUpdateStatus(
        context: TaskContext,
        taskStatus: TaskStatus,
        task: Task
    ): Result<out String?> {
        return when (taskStatus) {
            TaskStatus.Executing -> {
                val result = task.onExecuting(context)
                if (result.status == ResultStatus.SUCCESS) {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Awaiting)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Failed)
                    Result(
                        ResultStatus.FAILED,
                        result.output
                    )
                } else {
                    Result(
                        ResultStatus.AWAIT,
                        result.output
                    )
                }
            }
            TaskStatus.Awaiting -> {
                val result = task.onAwaiting(context)
                if (result.status == ResultStatus.SUCCESS) {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Idle)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Failed)
                    Result(
                        ResultStatus.FAILED,
                        result.output
                    )
                } else {
                    Result(
                        ResultStatus.AWAIT,
                        result.output
                    )
                }
            }
            TaskStatus.Idle -> {
                val result = task.onIdle(context)
                if (result.status == ResultStatus.FAILED) {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Failed)
                    Result(
                        ResultStatus.FAILED,
                        result.output
                    )
                } else if (Status.getNextOperatorTask(context.flinkCluster) != null) {
                    Status.selectNextTask(context.flinkCluster)
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else {
                    Result(
                        ResultStatus.AWAIT,
                        result.output
                    )
                }
            }
            TaskStatus.Failed -> {
                task.onFailed(context)
                Status.setClusterStatus(context.flinkCluster, ClusterStatus.Failed)
                Status.resetTasks(context.flinkCluster, listOf(ClusterTask.ClusterHalted))
                Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                Result(ResultStatus.FAILED, "Task failed")
            }
        }
    }

    private fun getOperatorTaskOrThrow(clusterTask: ClusterTask) =
        controller.taskHandlers[clusterTask] ?: throw RuntimeException("Implementation not found for task $clusterTask")
}
