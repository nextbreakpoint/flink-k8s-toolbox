package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class CreateBootstrapJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return taskFailedWithOutput(context.flinkCluster, "Cluster ${context.flinkCluster.metadata.name} doesn't have a job")
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.BOOTSTRAPPING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Failed to upload JAR file to cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val clusterResources = createClusterResources(context.clusterId, context.flinkCluster)

        val removeJarResponse = context.controller.removeJar(context.clusterId)

        if (!removeJarResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry removing old JAR files from cluster ${context.flinkCluster.metadata.name}...")
        }

        val bootstrapJobResponse = context.controller.createBootstrapJob(context.clusterId, clusterResources)

        if (bootstrapJobResponse.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "Uploading JAR file to cluster ${context.flinkCluster.metadata.name}...")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Retry uploading JAR file to cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return taskFailedWithOutput(context.flinkCluster, "Cluster ${context.flinkCluster.metadata.name} doesn't have a job")
        }

        val elapsedTime = context.controller.currentTimeMillis() - context.operatorTimestamp

        val seconds = elapsedTime / 1000

        if (elapsedTime > Timeout.BOOTSTRAPPING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "JAR file has not been uploaded to cluster ${context.flinkCluster.metadata.name} after $seconds seconds")
        }

        val response = context.controller.isJarReady(context.clusterId)

        if (response.isCompleted()) {
            return taskCompletedWithOutput(context.flinkCluster, "JAR file uploaded to cluster ${context.flinkCluster.metadata.name} in $seconds seconds")
        }

        return taskAwaitingWithOutput(context.flinkCluster, "Wait for JAR file of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}