package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.common.model.TaskStatus
import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorCommand
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorController
import com.nextbreakpoint.operator.OperatorResources
import com.nextbreakpoint.operator.task.*
import org.apache.log4j.Logger

class ClusterUpdateStatus(val controller: OperatorController, val resources: OperatorResources) : OperatorCommand<V1FlinkCluster, Void?>(controller.flinkOptions) {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterUpdateStatus::class.simpleName)

        private val operatorTasks = mapOf(
            OperatorTask.INITIALISE_CLUSTER to InitialiseCluster(),
            OperatorTask.TERMINATE_CLUSTER to TerminateCluster(),
            OperatorTask.SUSPEND_CLUSTER to SuspendCluster(),
            OperatorTask.HALT_CLUSTER to HaltCluster(),
            OperatorTask.RUN_CLUSTER to RunCluster(),
            OperatorTask.STARTING_CLUSTER to StartingCluster(),
            OperatorTask.STOPPING_CLUSTER to StoppingCluster(),
            OperatorTask.CHECKPOINTING_CLUSTER to CheckpointingCluster(),
            OperatorTask.CREATE_SAVEPOINT to CreateSavepoint(),
            OperatorTask.ERASE_SAVEPOINT to EraseSavepoint(),
            OperatorTask.CREATE_RESOURCES to CreateResources(),
            OperatorTask.DELETE_RESOURCES to DeleteResources(),
            OperatorTask.TERMINATE_PODS to TerminatePods(),
            OperatorTask.RESTART_PODS to RestartPods(),
            OperatorTask.DELETE_UPLOAD_JOB to DeleteUploadJob(),
            OperatorTask.UPLOAD_JAR to UploadJar(),
            OperatorTask.CANCEL_JOB to CancelJob(),
            OperatorTask.START_JOB to StartJob(),
            OperatorTask.STOP_JOB to StopJob()
        )
    }

    override fun execute(clusterId: ClusterId, params: V1FlinkCluster): Result<Void?> {
        try {
            logOperatorAnnotations(params)

            val lastUpdated = OperatorAnnotations.getOperatorTimestamp(params)

            val context = OperatorContext(
                lastUpdated, makeClusterId(params), params, controller, resources
            )

            val operatorStatus = OperatorAnnotations.getCurrentOperatorStatus(params)

            if (!OperatorAnnotations.hasCurrentOperatorTask(params)) {
                OperatorAnnotations.resetOperatorTasks(params, listOf(OperatorTask.INITIALISE_CLUSTER))

                controller.updateAnnotations(params)

                logger.info("Initialising cluster ${clusterId.name}...")

                return Result(ResultStatus.SUCCESS, null)
            } else {
                val operatorTask = OperatorAnnotations.getCurrentOperatorTask(params)

                val taskHandler = getOperatorTaskOrThrow(operatorTask)

                val taskResult = updateTask(context, operatorStatus, params, taskHandler)

                if (OperatorAnnotations.getOperatorTimestamp(params) != lastUpdated) {
                    controller.updateAnnotations(params)
                }

                if (taskResult.status == ResultStatus.SUCCESS) {
                    logger.info("Cluster ${clusterId.name} task ${operatorTask.name} - ${taskResult.output}")

                    return Result(ResultStatus.SUCCESS, null)
                } else if (taskResult.status == ResultStatus.AWAIT) {
                    logger.info("Cluster ${clusterId.name} task ${operatorTask.name} - ${taskResult.output}")

                    return Result(ResultStatus.AWAIT, null)
                } else {
                    logger.warn("Cluster ${clusterId.name} task ${operatorTask.name} - ${taskResult.output}")

                    return Result(ResultStatus.FAILED, null)
                }
            }
        } catch (e : Exception) {
            logger.error("An error occurred while updating cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }

    private fun updateTask(
        context: OperatorContext,
        taskStatus: TaskStatus,
        flinkCluster: V1FlinkCluster,
        taskHandler: TaskHandler
    ): Result<out String?> {
        return when (taskStatus) {
            TaskStatus.EXECUTING -> {
                val result = taskHandler.onExecuting(context)
                if (result.status == ResultStatus.SUCCESS) {
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.AWAITING)
                    Result(ResultStatus.SUCCESS, result.output)
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.FAILED)
                    Result(ResultStatus.FAILED, result.output)
                } else {
                    Result(ResultStatus.AWAIT, result.output)
                }
            }
            TaskStatus.AWAITING -> {
                val result = taskHandler.onAwaiting(context)
                if (result.status == ResultStatus.SUCCESS) {
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.IDLE)
                    Result(ResultStatus.SUCCESS, result.output)
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.FAILED)
                    Result(ResultStatus.FAILED, result.output)
                } else {
                    Result(ResultStatus.AWAIT, result.output)
                }
            }
            TaskStatus.IDLE -> {
                val result = taskHandler.onIdle(context)
                if (result.status == ResultStatus.FAILED) {
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.FAILED)
                    Result(ResultStatus.FAILED, result.output)
                }
                if (OperatorAnnotations.getNextOperatorTask(flinkCluster) != null) {
                    OperatorAnnotations.advanceOperatorTask(flinkCluster)
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.EXECUTING)
                    Result(ResultStatus.SUCCESS, result.output)
                } else {
                    Result(ResultStatus.AWAIT, result.output)
                }
            }
            TaskStatus.FAILED -> {
                taskHandler.onFailed(context)
                OperatorAnnotations.setClusterStatus(flinkCluster, ClusterStatus.FAILED)
                OperatorAnnotations.resetOperatorTasks(flinkCluster, listOf(OperatorTask.HALT_CLUSTER))
                OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.EXECUTING)
                Result(ResultStatus.FAILED, "Task failed")
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

    private fun makeClusterId(flinkCluster: V1FlinkCluster) =
        ClusterId(
            namespace = flinkCluster.metadata.namespace,
            name = flinkCluster.metadata.name,
            uuid = flinkCluster.metadata.uid
        )
}
