package com.nextbreakpoint.flinkoperator.controller.task

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.controller.core.Task
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import org.apache.log4j.Logger

class OnCheckpointing(logger: Logger) : Task(logger) {
    override fun execute(context: TaskContext) {
        val jobRunningResponse = context.isJobRunning(context.clusterId)

        if (!jobRunningResponse.isCompleted()) {
            logger.error("Job not running")

            context.setClusterStatus(ClusterStatus.Failed)

            return
        }

        val options = context.getSavepointOtions()

        val response = context.triggerSavepoint(context.clusterId, options)

        if (!response.isCompleted()) {
            logger.error("Savepoint request failed")

            context.setClusterStatus(ClusterStatus.Failed)

            return
        }

        logger.info("Savepoint requested created")

        context.setSavepointRequest(response.output)
        context.setClusterStatus(ClusterStatus.Running)
    }
}