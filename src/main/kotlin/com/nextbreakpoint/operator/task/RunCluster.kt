package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorContext

class RunCluster : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorAnnotations.setClusterStatus(context.flinkCluster, ClusterStatus.RUNNING)

        OperatorAnnotations.updateSavepointTimestamp(context.flinkCluster)

        return Result(ResultStatus.SUCCESS, "Cluster status updated")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(ResultStatus.SUCCESS, "Cluster running")
    }

    override fun onIdle(context: OperatorContext) {
        if (OperatorAnnotations.getNextOperatorTask(context.flinkCluster) == null) {
            val lastSavepointsTimestamp = OperatorAnnotations.getSavepointTimestamp(context.flinkCluster)

            if (System.currentTimeMillis() - lastSavepointsTimestamp > context.controller.savepointInterval) {
                // change status here to prevent start/stop commands to change the status
                OperatorAnnotations.setClusterStatus(context.flinkCluster, ClusterStatus.CHECKPOINTING)

                OperatorAnnotations.appendOperatorTask(context.flinkCluster, OperatorTask.CREATE_SAVEPOINT)
                OperatorAnnotations.appendOperatorTask(context.flinkCluster, OperatorTask.RUN_CLUSTER)
            }
        }
    }

    override fun onFailed(context: OperatorContext) {
    }
}