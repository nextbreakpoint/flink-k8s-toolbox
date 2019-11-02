package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorState
import com.nextbreakpoint.flinkoperator.controller.OperatorContext
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler

class TerminateCluster : OperatorTaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorState.setClusterStatus(context.flinkCluster, ClusterStatus.TERMINATED)
        OperatorState.setOperatorTaskAttempts(context.flinkCluster, 0)
        OperatorState.appendTasks(context.flinkCluster, listOf())

        return Result(
            ResultStatus.SUCCESS,
            "Status of cluster ${context.clusterId.name} has been updated"
        )
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.SUCCESS,
            "Cluster ${context.clusterId.name} is terminated"
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