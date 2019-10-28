package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
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

            val lastUpdated = OperatorAnnotations.getOperatorTimestamp(params)

            val context = OperatorContext(
                lastUpdated, clusterId, params, controller, resources
            )

            if (OperatorAnnotations.hasCurrentTask(params)) {
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
        OperatorAnnotations.appendTasks(context.flinkCluster, listOf(OperatorTask.INITIALISE_CLUSTER))

        controller.updateAnnotations(clusterId, context.flinkCluster.metadata.annotations)

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
        val taskStatus = OperatorAnnotations.getCurrentTaskStatus(context.flinkCluster)

        val currentTask = OperatorAnnotations.getCurrentTask(context.flinkCluster)

        logger.info("Cluster ${clusterId.name}, status ${taskStatus}, task ${currentTask.name}")

        val taskHandler = getOperatorTaskOrThrow(currentTask)

        val taskResult = updateTask(context, taskStatus, taskHandler)

        val currentTimestamp = OperatorAnnotations.getOperatorTimestamp(context.flinkCluster)

        if (currentTimestamp != context.lastUpdated) {
            controller.updateAnnotations(clusterId, context.flinkCluster.metadata.annotations)
        }

        if (OperatorAnnotations.getSavepointPath(context.flinkCluster) != context.flinkCluster.spec.flinkOperator.savepointPath) {
            controller.updateSavepoint(clusterId, OperatorAnnotations.getSavepointPath(context.flinkCluster) ?: "")
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
                    OperatorAnnotations.setTaskStatus(context.flinkCluster, TaskStatus.AWAITING)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorAnnotations.setTaskStatus(context.flinkCluster, TaskStatus.FAILED)
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
                    OperatorAnnotations.setTaskStatus(context.flinkCluster, TaskStatus.IDLE)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorAnnotations.setTaskStatus(context.flinkCluster, TaskStatus.FAILED)
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
                    OperatorAnnotations.setTaskStatus(context.flinkCluster, TaskStatus.FAILED)
                    Result(
                        ResultStatus.FAILED,
                        result.output
                    )
                } else if (OperatorAnnotations.getNextOperatorTask(context.flinkCluster) != null) {
                    OperatorAnnotations.selectNextTask(context.flinkCluster)
                    OperatorAnnotations.setTaskStatus(context.flinkCluster, TaskStatus.EXECUTING)
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
                OperatorAnnotations.setClusterStatus(context.flinkCluster, ClusterStatus.FAILED)
                OperatorAnnotations.resetTasks(context.flinkCluster, listOf(OperatorTask.CLUSTER_HALTED))
                OperatorAnnotations.setTaskStatus(context.flinkCluster, TaskStatus.EXECUTING)
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
