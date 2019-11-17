package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import io.kubernetes.client.models.V1StatefulSet
import org.apache.log4j.Logger

class UpdateStatus(
    private val controller: OperatorController
) : OperatorCommand<Void?, Void?>(
    controller.flinkOptions,
    controller.flinkContext,
    controller.kubernetesContext
) {
    companion object {
        private val logger: Logger = Logger.getLogger(UpdateStatus::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val flinkCluster = controller.cache.getFlinkCluster(clusterId)

            val resources = controller.cache.getResources()

            logOperatorAnnotations(flinkCluster)

            val operatorTimestamp = OperatorState.getOperatorTimestamp(flinkCluster)

            val actionTimestamp = OperatorAnnotations.getActionTimestamp(flinkCluster)

            val context = OperatorContext(
                operatorTimestamp, actionTimestamp, clusterId, flinkCluster, resources, controller
            )

            if (OperatorState.hasCurrentTask(flinkCluster)) {
                return update(clusterId, context)
            } else {
                return initialise(clusterId, context)
            }
        } catch (e : Exception) {
            logger.error("An error occurred while updating cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }

    private fun initialise(
        clusterId: ClusterId,
        context: OperatorContext
    ): Result<Void?> {
        OperatorState.appendTasks(context.flinkCluster, listOf(OperatorTask.InitialiseCluster))
        OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.Unknown)
        OperatorState.setTaskAttempts(context.flinkCluster, 0)
        OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.Executing)

        controller.updateState(clusterId, context.flinkCluster)

        logger.info("Initialising cluster ${clusterId.name}...")

        return Result(
            ResultStatus.SUCCESS,
            null
        )
    }

    private fun update(
        clusterId: ClusterId,
        context: OperatorContext
    ): Result<Void?> {
        val taskStatus = OperatorState.getCurrentTaskStatus(context.flinkCluster)

        val currentTask = OperatorState.getCurrentTask(context.flinkCluster)

        logger.info("Cluster ${clusterId.name}, status ${taskStatus}, task ${currentTask.name}")

        val taskHandler = getOperatorTaskOrThrow(currentTask)

        val taskResult = updateTask(context, taskStatus, taskHandler)

        val statefulSet = context.resources.taskmanagerStatefulSets[context.clusterId]

        updateStatusTaskManagers(context.flinkCluster, statefulSet)

        val operatorTimestamp = OperatorState.getOperatorTimestamp(context.flinkCluster)

        if (operatorTimestamp != context.operatorTimestamp) {
            controller.updateState(clusterId, context.flinkCluster)
        }

        val actionTimestamp = OperatorAnnotations.getActionTimestamp(context.flinkCluster)

        if (actionTimestamp != context.actionTimestamp) {
            controller.updateAnnotations(clusterId, context.flinkCluster)
        }

        if (OperatorState.getSavepointPath(context.flinkCluster) != context.flinkCluster.spec.operator.savepointPath) {
            controller.updateSavepoint(clusterId, OperatorState.getSavepointPath(context.flinkCluster) ?: "")
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
        if (OperatorState.getActiveTaskManagers(flinkCluster) != taskManagers) {
            OperatorState.setActiveTaskManagers(flinkCluster, taskManagers)
        }
        val taskSlots = flinkCluster.status?.taskSlots ?: 1
        if (OperatorState.getTotalTaskSlots(flinkCluster) != taskManagers * taskSlots) {
            OperatorState.setTotalTaskSlots(flinkCluster,taskManagers * taskSlots)
        }
    }

    private fun updateTask(
        context: OperatorContext,
        taskStatus: TaskStatus,
        taskHandler: OperatorTaskHandler
    ): Result<out String?> {
        return when (taskStatus) {
            TaskStatus.Executing -> {
                val result = taskHandler.onExecuting(context)
                if (result.status == ResultStatus.SUCCESS) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.Awaiting)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.Failed)
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
                val result = taskHandler.onAwaiting(context)
                if (result.status == ResultStatus.SUCCESS) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.Idle)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.Failed)
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
                val result = taskHandler.onIdle(context)
                if (result.status == ResultStatus.FAILED) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.Failed)
                    Result(
                        ResultStatus.FAILED,
                        result.output
                    )
                } else if (OperatorState.getNextOperatorTask(context.flinkCluster) != null) {
                    OperatorState.selectNextTask(context.flinkCluster)
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
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
                taskHandler.onFailed(context)
                OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.Failed)
                OperatorState.resetTasks(context.flinkCluster, listOf(OperatorTask.ClusterHalted))
                OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                OperatorAnnotations.setManualAction(context.flinkCluster, ManualAction.NONE)
                Result(
                    ResultStatus.FAILED,
                    "Task failed"
                )
            }
        }
    }

    private fun logOperatorAnnotations(flinkCluster: V1FlinkCluster) {
        logger.debug("Cluster ${flinkCluster.metadata.name}")
        val annotations = flinkCluster.metadata.annotations ?: mapOf<String, String>()
        annotations.forEach { (key, value) -> logger.debug("$key = $value") }
    }

    private fun getOperatorTaskOrThrow(clusterTask: OperatorTask) =
        controller.taskHandlers.get(clusterTask) ?: throw RuntimeException("Unsupported task $clusterTask")
}
