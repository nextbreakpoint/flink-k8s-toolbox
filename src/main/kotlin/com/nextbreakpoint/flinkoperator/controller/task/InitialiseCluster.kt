package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.OperatorTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler

class InitialiseCluster : OperatorTaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.Starting)
        OperatorState.setTaskAttempts(context.flinkCluster, 0)

        if (context.flinkCluster.spec.flinkJob != null) {
            OperatorState.appendTasks(context.flinkCluster,
                listOf(
                    OperatorTask.CreateResources,
                    OperatorTask.CreateUploadJob,
                    OperatorTask.StartJob,
                    OperatorTask.ClusterRunning
                )
            )
        } else {
            OperatorState.appendTasks(context.flinkCluster,
                listOf(
                    OperatorTask.CreateResources,
                    OperatorTask.ClusterRunning
                )
            )
        }

        val jobManagerDigest = CustomResources.computeDigest(context.flinkCluster.spec?.jobManager)
        val taskManagerDigest = CustomResources.computeDigest(context.flinkCluster.spec?.taskManager)
        val flinkImageDigest = CustomResources.computeDigest(context.flinkCluster.spec?.flinkImage)
        val flinkJobDigest = CustomResources.computeDigest(context.flinkCluster.spec?.flinkJob)

        OperatorState.setJobManagerDigest(context.flinkCluster, jobManagerDigest)
        OperatorState.setTaskManagerDigest(context.flinkCluster, taskManagerDigest)
        OperatorState.setFlinkImageDigest(context.flinkCluster, flinkImageDigest)
        OperatorState.setFlinkJobDigest(context.flinkCluster, flinkJobDigest)

        OperatorState.setTaskManagers(context.flinkCluster, 0)

        val taskManagers = context.flinkCluster.spec?.taskManagers ?: 1
        val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
        OperatorState.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)

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