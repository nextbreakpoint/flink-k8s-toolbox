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

        val flinkClusterSpecDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec)
        val jobManagerSpecDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec.jobManager)
        val taskManagerSpecDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec.taskManager)
        val flinkImageSpecDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec.flinkImage)
        val flinkJobSpecDigest = FlinkClusterSpecification.computeDigest(context.flinkCluster.spec.flinkJob)

        OperatorAnnotations.setFlinkClusterSpecDigest(context.flinkCluster, flinkClusterSpecDigest)
        OperatorAnnotations.setJobManagerSpecDigest(context.flinkCluster, jobManagerSpecDigest)
        OperatorAnnotations.setTaskManagerSpecDigest(context.flinkCluster, taskManagerSpecDigest)
        OperatorAnnotations.setFlinkImageSpecDigest(context.flinkCluster, flinkImageSpecDigest)
        OperatorAnnotations.setFlinkJobSpecDigest(context.flinkCluster, flinkJobSpecDigest)

        return Result(ResultStatus.SUCCESS, "Cluster status updated")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        return Result(ResultStatus.SUCCESS, "Cluster initialized")
    }

    override fun onIdle(context: OperatorContext) {
    }

    override fun onFailed(context: OperatorContext) {
    }
}