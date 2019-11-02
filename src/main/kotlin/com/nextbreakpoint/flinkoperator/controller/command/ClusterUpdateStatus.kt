package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import org.apache.log4j.Logger

class ClusterUpdateStatus(val controller: OperatorController, val resources: OperatorResources, val operatorTasks: Map<OperatorTask, OperatorTaskHandler>) : OperatorCommand<V1FlinkCluster, Void?>(controller.flinkOptions, controller.flinkContext, controller.kubernetesContext) {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterUpdateStatus::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: V1FlinkCluster): Result<Void?> {
        try {
            logOperatorAnnotations(params)

            val lastUpdated = OperatorState.getOperatorTimestamp(params)

            val context = OperatorContext(
                lastUpdated, clusterId, params, controller, resources
            )

            if (OperatorState.hasCurrentTask(params)) {
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
        OperatorState.appendTasks(context.flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))
        OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.UNKNOWN)
        OperatorState.setOperatorTaskAttempts(context.flinkCluster, 0)
        OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.EXECUTING)

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

        val currentTimestamp = OperatorState.getOperatorTimestamp(context.flinkCluster)

        if (currentTimestamp != context.lastUpdated) {
            controller.updateState(clusterId, context.flinkCluster)
        }

        if (OperatorState.getSavepointPath(context.flinkCluster) != context.flinkCluster.spec.flinkOperator.savepointPath) {
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

    private fun updateTask(
        context: OperatorContext,
        taskStatus: TaskStatus,
        taskHandler: OperatorTaskHandler
    ): Result<out String?> {
        return when (taskStatus) {
            TaskStatus.EXECUTING -> {
                val result = taskHandler.onExecuting(context)
                if (result.status == ResultStatus.SUCCESS) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.AWAITING)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.FAILED)
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
            TaskStatus.AWAITING -> {
                val result = taskHandler.onAwaiting(context)
                if (result.status == ResultStatus.SUCCESS) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.IDLE)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.FAILED)
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
            TaskStatus.IDLE -> {
                val result = taskHandler.onIdle(context)
                if (result.status == ResultStatus.FAILED) {
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.FAILED)
                    Result(
                        ResultStatus.FAILED,
                        result.output
                    )
                } else if (OperatorState.getNextOperatorTask(context.flinkCluster) != null) {
                    OperatorState.selectNextTask(context.flinkCluster)
                    OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.EXECUTING)
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
            TaskStatus.FAILED -> {
                taskHandler.onFailed(context)
                OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.FAILED)
                OperatorState.resetTasks(context.flinkCluster, listOf(OperatorTask.CLUSTER_HALTED))
                OperatorState.setTaskStatus(context.flinkCluster, TaskStatus.EXECUTING)
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
        operatorTasks.get(clusterTask) ?: throw RuntimeException("Unsupported task $clusterTask")
}
