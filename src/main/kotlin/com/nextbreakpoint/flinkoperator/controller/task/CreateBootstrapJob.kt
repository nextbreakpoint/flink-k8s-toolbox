package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class CreateBootstrapJob : Task {
    override fun onExecuting(context: TaskContext): Result<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return taskFailedWithOutput(context.flinkCluster, "Bootstrap job not defined")
        }

        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.BOOTSTRAPPING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val removeJarResponse = context.removeJar(context.clusterId)

        if (!removeJarResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry removing old JAR files...")
        }

        val resources = createClusterResources(context.clusterId, context.flinkCluster)

        val createBootstrapJobResponse = context.createBootstrapJob(context.clusterId, resources)

        if (!createBootstrapJobResponse.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Retry creating bootstrap job...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "Bootstrap job created...")
    }

    override fun onAwaiting(context: TaskContext): Result<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.BOOTSTRAPPING_JOB_TIMEOUT) {
            return taskFailedWithOutput(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val response = context.isJarReady(context.clusterId)

        if (!response.isCompleted()) {
            return taskAwaitingWithOutput(context.flinkCluster, "Wait until JAR file is uploaded...")
        }

        return taskCompletedWithOutput(context.flinkCluster, "JAR file uploaded in $seconds seconds")
    }

    override fun onIdle(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }

    override fun onFailed(context: TaskContext): Result<String> {
        return taskAwaitingWithOutput(context.flinkCluster, "")
    }
}