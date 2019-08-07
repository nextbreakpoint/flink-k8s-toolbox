package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorContext

class SuspendCluster : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorAnnotations.setClusterStatus(context.flinkCluster, ClusterStatus.SUSPENDED)
        OperatorAnnotations.setOperatorTaskAttempts(context.flinkCluster, 0)
        OperatorAnnotations.appendOperatorTasks(context.flinkCluster, listOf())

        return Result(ResultStatus.SUCCESS, "Cluster status updated")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(ResultStatus.SUCCESS, "Cluster suspended")
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }
}