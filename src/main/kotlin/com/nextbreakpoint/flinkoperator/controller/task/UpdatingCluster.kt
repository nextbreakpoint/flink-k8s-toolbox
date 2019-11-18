package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext

class UpdatingCluster : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Updating)
        Status.setTaskAttempts(context.flinkCluster, 0)

        return Result(
            ResultStatus.SUCCESS,
            "Status of cluster ${context.clusterId.name} has been updated"
        )
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        return Result(
            ResultStatus.SUCCESS,
            "Cluster ${context.clusterId.name} is updating..."
        )
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return Result(
            ResultStatus.AWAIT,
            ""
        )
    }
}