package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.FlinkClusterSpecification
import com.nextbreakpoint.common.model.ClusterStatus
import com.nextbreakpoint.common.model.OperatorTask
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.operator.OperatorAnnotations
import com.nextbreakpoint.operator.OperatorContext

class InitialiseCluster : TaskHandler {
    override fun onExecuting(context: OperatorContext): Result<String> {
        OperatorAnnotations.setClusterStatus(context.flinkCluster, ClusterStatus.STARTING)

        OperatorAnnotations.appendOperatorTask(context.flinkCluster, OperatorTask.CREATE_RESOURCES)
        OperatorAnnotations.appendOperatorTask(context.flinkCluster, OperatorTask.UPLOAD_JAR)
        OperatorAnnotations.appendOperatorTask(context.flinkCluster, OperatorTask.START_JOB)
        OperatorAnnotations.appendOperatorTask(context.flinkCluster, OperatorTask.RUN_CLUSTER)

        val jobManagerDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec?.jobManager)
        val taskManagerDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec?.taskManager)
        val flinkImageDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec?.flinkImage)
        val flinkJobDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec?.flinkJob)

        OperatorAnnotations.setJobManagerDigest(context.flinkCluster, jobManagerDigest)
        OperatorAnnotations.setTaskManagerDigest(context.flinkCluster, taskManagerDigest)
        OperatorAnnotations.setFlinkImageDigest(context.flinkCluster, flinkImageDigest)
        OperatorAnnotations.setFlinkJobDigest(context.flinkCluster, flinkJobDigest)

        return Result(ResultStatus.SUCCESS, "Cluster status updated")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(ResultStatus.SUCCESS, "Cluster initialized")
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }
}