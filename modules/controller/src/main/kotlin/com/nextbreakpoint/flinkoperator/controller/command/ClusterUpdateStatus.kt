package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorController
import com.nextbreakpoint.flinkoperator.controller.OperatorResources
import com.nextbreakpoint.flinkoperator.controller.task.CancelJob
import com.nextbreakpoint.flinkoperator.controller.task.CheckpointingCluster
import com.nextbreakpoint.flinkoperator.controller.task.CreateResources
import com.nextbreakpoint.flinkoperator.controller.task.CreateSavepoint
import com.nextbreakpoint.flinkoperator.controller.task.DeleteResources
import com.nextbreakpoint.flinkoperator.controller.task.DeleteUploadJob
import com.nextbreakpoint.flinkoperator.controller.task.EraseSavepoint
import com.nextbreakpoint.flinkoperator.controller.task.ClusterHalted
import com.nextbreakpoint.flinkoperator.controller.task.InitialiseCluster
import com.nextbreakpoint.flinkoperator.controller.task.RestartPods
import com.nextbreakpoint.flinkoperator.controller.task.ClusterRunning
import com.nextbreakpoint.flinkoperator.controller.task.StartJob
import com.nextbreakpoint.flinkoperator.controller.task.StartingCluster
import com.nextbreakpoint.flinkoperator.controller.task.StopJob
import com.nextbreakpoint.flinkoperator.controller.task.StoppingCluster
import com.nextbreakpoint.flinkoperator.controller.task.SuspendCluster
import com.nextbreakpoint.flinkoperator.controller.task.TerminateCluster
import com.nextbreakpoint.flinkoperator.controller.task.TerminatePods
import com.nextbreakpoint.flinkoperator.controller.task.UploadJar
import org.apache.log4j.Logger

class ClusterUpdateStatus(val controller: OperatorController, val resources: OperatorResources) : OperatorCommand<V1FlinkCluster, Void?>(controller.flinkOptions) {
    companion object {
        private val logger: Logger = Logger.getLogger(ClusterUpdateStatus::class.simpleName)

        private val operatorTasks = mapOf(
            OperatorTask.INITIALISE_CLUSTER to InitialiseCluster(),
            OperatorTask.TERMINATE_CLUSTER to TerminateCluster(),
            OperatorTask.SUSPEND_CLUSTER to SuspendCluster(),
            OperatorTask.CLUSTER_HALTED to ClusterHalted(),
            OperatorTask.CLUSTER_RUNNING to ClusterRunning(),
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
                OperatorAnnotations.appendOperatorTasks(params, listOf(OperatorTask.INITIALISE_CLUSTER))

                controller.updateAnnotations(clusterId, params.metadata.annotations)

                logger.info("Initialising cluster ${clusterId.name}...")

                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            } else {
                val operatorTask = OperatorAnnotations.getCurrentOperatorTask(params)

                logger.info("Cluster ${clusterId.name}, status ${operatorStatus}, task ${operatorTask.name}")

                val taskHandler = getOperatorTaskOrThrow(operatorTask)

                val taskResult = updateTask(context, operatorStatus, params, taskHandler)

                val currentTimestamp = OperatorAnnotations.getOperatorTimestamp(params)

                if (currentTimestamp != lastUpdated) {
                    controller.updateAnnotations(clusterId, params.metadata.annotations)
                }

                if (currentTimestamp - System.currentTimeMillis() < 30000 && OperatorAnnotations.getSavepointPath(params) != params.spec.flinkOperator.savepointPath) {
                    controller.updateSavepoint(clusterId, OperatorAnnotations.getSavepointPath(params) ?: "")
                }

                if (taskResult.status == ResultStatus.SUCCESS) {
                    if (taskResult.output?.isNotBlank() == true) logger.info(taskResult.output)

                    return Result(
                        ResultStatus.SUCCESS,
                        null
                    )
                } else if (taskResult.status == ResultStatus.AWAIT) {
                    if (taskResult.output?.isNotBlank() == true) logger.info(taskResult.output)

                    return Result(
                        ResultStatus.AWAIT,
                        null
                    )
                } else {
                    if (taskResult.output?.isNotBlank() == true) logger.warn(taskResult.output)

                    return Result(
                        ResultStatus.FAILED,
                        null
                    )
                }
            }
        } catch (e : Exception) {
            logger.error("An error occurred while updating cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }

    private fun updateTask(
        context: OperatorContext,
        taskStatus: TaskStatus,
        flinkCluster: V1FlinkCluster,
        taskHandler: OperatorTaskHandler
    ): Result<out String?> {
        return when (taskStatus) {
            TaskStatus.EXECUTING -> {
                val result = taskHandler.onExecuting(context)
                if (result.status == ResultStatus.SUCCESS) {
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.AWAITING)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.FAILED)
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
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.IDLE)
                    Result(
                        ResultStatus.SUCCESS,
                        result.output
                    )
                } else if (result.status == ResultStatus.FAILED) {
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.FAILED)
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
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.FAILED)
                    Result(
                        ResultStatus.FAILED,
                        result.output
                    )
                }
                if (OperatorAnnotations.getNextOperatorTask(flinkCluster) != null) {
                    OperatorAnnotations.advanceOperatorTask(flinkCluster)
                    OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.EXECUTING)
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
                OperatorAnnotations.setClusterStatus(flinkCluster, ClusterStatus.FAILED)
                OperatorAnnotations.resetOperatorTasks(flinkCluster, listOf(OperatorTask.CLUSTER_HALTED))
                OperatorAnnotations.setOperatorStatus(flinkCluster, TaskStatus.EXECUTING)
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

    private fun makeClusterId(flinkCluster: V1FlinkCluster) =
        ClusterId(
            namespace = flinkCluster.metadata.namespace,
            name = flinkCluster.metadata.name,
            uuid = flinkCluster.metadata.uid
        )
}
