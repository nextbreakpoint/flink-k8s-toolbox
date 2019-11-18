package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.CustomResources
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorTask

class InitialiseCluster : OperatorTask {
    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.Starting)
        OperatorState.setTaskAttempts(context.flinkCluster, 0)

        if (context.flinkCluster.spec.bootstrap != null) {
            OperatorState.appendTasks(context.flinkCluster,
                listOf(
                    ClusterTask.CreateResources,
                    ClusterTask.CreateBootstrapJob,
                    ClusterTask.StartJob,
                    ClusterTask.ClusterRunning
                )
            )
        } else {
            OperatorState.appendTasks(context.flinkCluster,
                listOf(
                    ClusterTask.CreateResources,
                    ClusterTask.ClusterRunning
                )
            )
        }

        val jobManagerDigest = CustomResources.computeDigest(context.flinkCluster.spec?.jobManager)
        val taskManagerDigest = CustomResources.computeDigest(context.flinkCluster.spec?.taskManager)
        val runtimeDigest = CustomResources.computeDigest(context.flinkCluster.spec?.runtime)
        val bootstrapDigest = CustomResources.computeDigest(context.flinkCluster.spec?.bootstrap)

        OperatorState.setJobManagerDigest(context.flinkCluster, jobManagerDigest)
        OperatorState.setTaskManagerDigest(context.flinkCluster, taskManagerDigest)
        OperatorState.setRuntimeDigest(context.flinkCluster, runtimeDigest)
        OperatorState.setBootstrapDigest(context.flinkCluster, bootstrapDigest)

        val taskManagers = context.flinkCluster.spec?.taskManagers ?: 0
        val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
        OperatorState.setTaskManagers(context.flinkCluster, taskManagers)
        OperatorState.setTaskSlots(context.flinkCluster, taskSlots)
        OperatorState.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)

        val savepointPath = context.flinkCluster.spec?.operator?.savepointPath
        OperatorState.setSavepointPath(context.flinkCluster, savepointPath)

        val labelSelector = CustomResources.makeLabelSelector(context.clusterId)
        OperatorState.setLabelSelector(context.flinkCluster, labelSelector)

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