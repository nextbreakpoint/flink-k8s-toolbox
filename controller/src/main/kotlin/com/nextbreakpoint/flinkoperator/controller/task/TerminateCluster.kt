package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorTaskHandler
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorContext

class TerminateCluster : OperatorTaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorAnnotations.setClusterStatus(context.flinkCluster, ClusterStatus.TERMINATED)
        OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, 0)
        OperatorAnnotations.appendOperatorTasks(context.flinkCluster, listOf())

        return Result(
            ResultStatus.SUCCESS,
            "Cluster status updated"
        )
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(
            ResultStatus.SUCCESS,
            "Cluster terminated"
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