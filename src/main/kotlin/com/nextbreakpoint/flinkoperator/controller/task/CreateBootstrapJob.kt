package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesBuilder
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResourcesValidator
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory
import org.apache.log4j.Logger

class CreateBootstrapJob : Task {
    companion object {
        private val logger: Logger = Logger.getLogger(CreateBootstrapJob::class.simpleName)
    }

    private val statusEvaluator = ClusterResourcesValidator()

    override fun onExecuting(context: TaskContext): Result<String> {
        if (context.flinkCluster.spec?.bootstrap == null) {
            return Result(
                ResultStatus.FAILED,
                "Cluster ${context.flinkCluster.metadata.name} doesn't have a job"
            )
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.BOOTSTRAPPING_JOB_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "Failed to upload JAR file to cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

        val clusterResources = ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            context.flinkCluster.metadata.namespace,
            context.clusterId.uuid,
            "flink-operator",
            context.flinkCluster
        ).build()

        val removeJarResponse = context.controller.removeJar(context.clusterId)

        if (removeJarResponse.status != ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.AWAIT,
                "Retry removing old JAR files from cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        val bootstrapJobResponse = context.controller.createBootstrapJob(context.clusterId, clusterResources)

        if (bootstrapJobResponse.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "Uploading JAR file to cluster ${context.flinkCluster.metadata.name}..."
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Retry uploading JAR file to cluster ${context.flinkCluster.metadata.name}..."
        )
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        if (elapsedTime > Timeout.BOOTSTRAPPING_JOB_TIMEOUT) {
            return Result(
                ResultStatus.FAILED,
                "JAR file has not been uploaded to cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds"
            )
        }

//        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)
//
//        if (context.hasBootstrapJobDiverged(clusterStatus)) {
//            logger.info(clusterStatus.bootstrapJob.toString())
//
//            return Result(
//                ResultStatus.AWAIT,
//                "Resources of cluster ${context.flinkCluster.metadata.name} are not ready..."
//            )
//        }

        val response = context.controller.isJarReady(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(
                ResultStatus.SUCCESS,
                "JAR file uploaded to cluster ${context.flinkCluster.metadata.name} in ${elapsedTime / 1000} seconds"
            )
        }

        return Result(
            ResultStatus.AWAIT,
            "Wait for JAR file of cluster ${context.flinkCluster.metadata.name}..."
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

//    private fun evaluateClusterStatus(clusterId: ClusterId, cluster: V1FlinkCluster, resources: OperatorResources): ClusterResourcesStatus {
//        val bootstrapJob = resources.bootstrapJobs.get(clusterId)
//        val jobmnagerService = resources.jobmanagerServices.get(clusterId)
//        val jobmanagerStatefulSet = resources.jobmanagerStatefulSets.get(clusterId)
//        val taskmanagerStatefulSet = resources.taskmanagerStatefulSets.get(clusterId)
//
//        val actualResources = ClusterResources(
//            bootstrapJob = bootstrapJob,
//            jobmanagerService = jobmnagerService,
//            jobmanagerStatefulSet = jobmanagerStatefulSet,
//            taskmanagerStatefulSet = taskmanagerStatefulSet
//        )
//
//        return statusEvaluator.evaluate(clusterId, cluster, actualResources)
//    }
}