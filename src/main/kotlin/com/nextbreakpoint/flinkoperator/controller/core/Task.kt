package com.nextbreakpoint.flinkoperator.controller.core

import org.apache.log4j.Logger

abstract class Task(val logger: Logger) {
    abstract fun execute(context: TaskContext)

    protected fun cancel(context: TaskContext): Boolean {
        if (context.doesBootstrapExists()) {
            val bootstrapResult = context.deleteBootstrapJob(context.clusterId)

            if (bootstrapResult.isCompleted()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        if (context.isSavepointRequired()) {
            val savepointRequest = context.getSavepointRequest()

            if (savepointRequest == null) {
                val options = context.getSavepointOtions()

                val cancelResult = context.cancelJob(context.clusterId, options)

                if (cancelResult.isFailed()) {
                    logger.error("Failed to cancel job...")

                    return true
                }

                if (cancelResult.isCompleted()) {
                    logger.info("Cancelling job with savepoint...")

                    context.setSavepointRequest(cancelResult.output)

                    return false
                }
            } else {
                val savepointResult = context.getLatestSavepoint(context.clusterId, savepointRequest)

                if (savepointResult.isFailed()) {
                    logger.error("Failed to cancel job...")

                    return true
                }

                if (savepointResult.isCompleted()) {
                    logger.info("Savepoint created for job ${savepointRequest.jobId} (${savepointResult.output})")

                    context.setSavepointPath(savepointResult.output)

                    return true
                }
            }
        } else {
            logger.info("Savepoint not required")

            val stopResult = context.stopJob(context.clusterId)

            if (stopResult.isFailed()) {
                logger.error("Failed to stop job...")

                return true
            }

            if (stopResult.isCompleted()) {
                logger.info("Stopping job without savepoint...")

                return true
            }
        }

        return false
    }

    protected fun suspend(context: TaskContext): Boolean {
        val terminatedResult = context.arePodsTerminated(context.clusterId)

        if (!terminatedResult.isCompleted()) {
            context.terminatePods(context.clusterId)

            return false
        }

        val bootstrapExists = context.doesBootstrapExists()

        if (bootstrapExists) {
            val bootstrapResult = context.deleteBootstrapJob(context.clusterId)

            if (bootstrapResult.isCompleted()) {
                logger.info("Bootstrap job deleted")
            }
        }

        val jobmanagerServiceExists = context.doesJobManagerServiceExists()

        if (jobmanagerServiceExists) {
            val serviceResult = context.deleteJobManagerService(context.clusterId)

            if (serviceResult.isCompleted()) {
                logger.info("JobManager service deleted")
            }
        }

        return !bootstrapExists && !jobmanagerServiceExists
    }

    protected fun terminate(context: TaskContext): Boolean {
        val terminatedResult = context.arePodsTerminated(context.clusterId)

        if (!terminatedResult.isCompleted()) {
            context.terminatePods(context.clusterId)

            return false
        }

        val bootstrapExists = context.doesBootstrapExists()
        val jobmanagerServiceExists = context.doesJobManagerServiceExists()
        val jobmanagerStatefuleSetExists = context.doesJobManagerStatefulSetExists()
        val taskmanagerStatefulSetExists = context.doesTaskManagerStatefulSetExists()
        val jomanagerPVCExists = context.doesJobManagerPVCExists()
        val taskmanagerPVCExists = context.doesTaskManagerPVCExists()

        if (bootstrapExists) {
            val bootstrapResult = context.deleteBootstrapJob(context.clusterId)

            if (bootstrapResult.isCompleted()) {
                logger.info("Bootstrap job deleted")
            }
        }

        if (jobmanagerServiceExists) {
            val serviceResult = context.deleteJobManagerService(context.clusterId)

            if (serviceResult.isCompleted()) {
                logger.info("JobManager service deleted")
            }
        }

        if (jobmanagerStatefuleSetExists || taskmanagerStatefulSetExists) {
            val statefulSetsResult = context.deleteStatefulSets(context.clusterId)

            if (statefulSetsResult.isCompleted()) {
                logger.info("JobManager and TaskManager statefulset deleted")
            }
        }

        if (jomanagerPVCExists || taskmanagerPVCExists) {
            val persistenVolumeClaimsResult = context.deletePersistentVolumeClaims(context.clusterId)

            if (persistenVolumeClaimsResult.isCompleted()) {
                logger.info("JobManager and TaskManager PVC deleted")
            }
        }

        return !bootstrapExists && !jobmanagerServiceExists && !jobmanagerStatefuleSetExists && !taskmanagerStatefulSetExists && !jomanagerPVCExists && !taskmanagerPVCExists
    }
}