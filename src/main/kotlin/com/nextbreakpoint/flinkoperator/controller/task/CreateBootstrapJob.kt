package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.TaskResult
import com.nextbreakpoint.flinkoperator.controller.core.Timeout

class CreateBootstrapJob : Task {
    override fun onExecuting(context: TaskContext): TaskResult<String> {
        if (!isBootstrapJobDefined(context.flinkCluster)) {
            return skip(context.flinkCluster, "Bootstrap job not defined")
        }

        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.BOOTSTRAPPING_JOB_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val removeJarResponse = context.removeJar(context.clusterId)

        if (!removeJarResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Retry removing old JAR files...")
        }

        val bootstrapJob = makeBootstrapJob(context.clusterId, context.flinkCluster)

        val createBootstrapJobResponse = context.createBootstrapJob(context.clusterId, bootstrapJob)

        if (!createBootstrapJobResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Retry creating bootstrap job...")
        }

        return next(context.flinkCluster, "Bootstrap job created...")
    }

    override fun onAwaiting(context: TaskContext): TaskResult<String> {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.BOOTSTRAPPING_JOB_TIMEOUT) {
            return fail(context.flinkCluster, "Operation timeout after $seconds seconds!")
        }

        val jarReadyResponse = context.isJarReady(context.clusterId)

        if (!jarReadyResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Waiting for JAR file...")
        }

        val jobStartedResponse = context.isJobStarted(context.clusterId)

        if (!jobStartedResponse.isCompleted()) {
            return repeat(context.flinkCluster, "Waiting for job...")
        }

        return next(context.flinkCluster, "JAR file uploaded after $seconds seconds")
    }

    override fun onIdle(context: TaskContext): TaskResult<String> {
        return next(context.flinkCluster, "Bootstrap job completed")
    }
}