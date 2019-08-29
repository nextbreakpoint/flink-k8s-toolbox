package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler

class InitialiseCluster : OperatorTaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorAnnotations.setClusterStatus(context.flinkCluster, ClusterStatus.STARTING)
        OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, 0)

        if (context.flinkCluster.spec.flinkJob != null) {
            OperatorAnnotations.appendTasks(context.flinkCluster,
                listOf(
                    OperatorTask.CREATE_RESOURCES,
                    OperatorTask.UPLOAD_JAR,
                    OperatorTask.START_JOB,
                    OperatorTask.CLUSTER_RUNNING
                )
            )
        } else {
            OperatorAnnotations.appendTasks(context.flinkCluster,
                listOf(
                    OperatorTask.CREATE_RESOURCES,
                    OperatorTask.CLUSTER_RUNNING
                )
            )
        }

        val jobManagerDigest = CustomResources.computeDigest(context.flinkCluster.spec?.jobManager)
        val taskManagerDigest = CustomResources.computeDigest(context.flinkCluster.spec?.taskManager)
        val flinkImageDigest = CustomResources.computeDigest(context.flinkCluster.spec?.flinkImage)
        val flinkJobDigest = CustomResources.computeDigest(context.flinkCluster.spec?.flinkJob)

        OperatorAnnotations.setJobManagerDigest(context.flinkCluster, jobManagerDigest)
        OperatorAnnotations.setTaskManagerDigest(context.flinkCluster, taskManagerDigest)
        OperatorAnnotations.setFlinkImageDigest(context.flinkCluster, flinkImageDigest)
        OperatorAnnotations.setFlinkJobDigest(context.flinkCluster, flinkJobDigest)

        return Result(
            ResultStatus.SUCCESS,
            "Status of cluster ${context.clusterId.name} has been updated"
        )
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.SUCCESS,
            "Cluster ${context.clusterId.name} initialized"
        )
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }
}