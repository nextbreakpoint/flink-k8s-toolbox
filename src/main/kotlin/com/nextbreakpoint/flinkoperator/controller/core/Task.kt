package com.nextbreakpoint.flinkoperator.controller.core

import org.apache.log4j.Logger

abstract class Task(val logger: Logger) {
    abstract fun execute(context: TaskContext)

    protected fun cancel(context: TaskContext): Boolean {
        if (context.doesBootstrapJobExists()) {
            val bootstrapResult = context.deleteBootstrapJob(context.clusterSelector)

            if (bootstrapResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        if (context.isSavepointRequired()) {
            val savepointRequest = context.getSavepointRequest()

            if (savepointRequest == null) {
                val options = context.getSavepointOtions()

                val cancelResult = context.cancelJob(context.clusterSelector, options)

                if (cancelResult.isSuccessful()) {
                    if (cancelResult.output != null) {
                        logger.info("Cancelling job with savepoint...")

                        context.setSavepointRequest(cancelResult.output)
                    } else {
                        logger.info("Stopping job without savepoint...")

                        return true
                    }
                } else {
                    logger.warn("Can't create savepoint. Retrying...")
                }
            } else {
                val savepointResult = context.getLatestSavepoint(context.clusterSelector, savepointRequest)

                logger.info("Savepoint is in progress...")

                if (savepointResult.isSuccessful()) {
                    logger.info("Savepoint created for job ${savepointRequest.jobId} (${savepointResult.output})")

                    context.setSavepointPath(savepointResult.output)

                    return true
                }
            }
        } else {
            logger.info("Savepoint not required")

            val stopResult = context.stopJob(context.clusterSelector)

            if (stopResult.isSuccessful()) {
                logger.info("Stopping job without savepoint...")

                return true
            }
        }

        return false
    }

    protected fun suspend(context: TaskContext): Boolean {
        val terminatedResult = context.arePodsTerminated(context.clusterSelector)

        if (!terminatedResult.output) {
            context.terminatePods(context.clusterSelector)

            return false
        }

        val bootstrapExists = context.doesBootstrapJobExists()

        if (bootstrapExists) {
            val bootstrapResult = context.deleteBootstrapJob(context.clusterSelector)

            if (bootstrapResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }
        }

        val jobmanagerServiceExists = context.doesJobManagerServiceExists()

        if (jobmanagerServiceExists) {
            val serviceResult = context.deleteJobManagerService(context.clusterSelector)

            if (serviceResult.isSuccessful()) {
                logger.info("JobManager service deleted")
            }
        }

        return !bootstrapExists && !jobmanagerServiceExists
    }

    protected fun terminate(context: TaskContext): Boolean {
        val terminatedResult = context.arePodsTerminated(context.clusterSelector)

        if (!terminatedResult.output) {
            context.terminatePods(context.clusterSelector)

            return false
        }

        val bootstrapExists = context.doesBootstrapJobExists()
        val jobmanagerServiceExists = context.doesJobManagerServiceExists()
        val jobmanagerStatefuleSetExists = context.doesJobManagerStatefulSetExists()
        val taskmanagerStatefulSetExists = context.doesTaskManagerStatefulSetExists()
        val jomanagerPVCExists = context.doesJobManagerPVCExists()
        val taskmanagerPVCExists = context.doesTaskManagerPVCExists()

        if (bootstrapExists) {
            val bootstrapResult = context.deleteBootstrapJob(context.clusterSelector)

            if (bootstrapResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }
        }

        if (jobmanagerServiceExists) {
            val serviceResult = context.deleteJobManagerService(context.clusterSelector)

            if (serviceResult.isSuccessful()) {
                logger.info("JobManager service deleted")
            }
        }

        if (jobmanagerStatefuleSetExists || taskmanagerStatefulSetExists) {
            val statefulSetsResult = context.deleteStatefulSets(context.clusterSelector)

            if (statefulSetsResult.isSuccessful()) {
                logger.info("JobManager and TaskManager statefulset deleted")
            }
        }

        if (jomanagerPVCExists || taskmanagerPVCExists) {
            val persistenVolumeClaimsResult = context.deletePersistentVolumeClaims(context.clusterSelector)

            if (persistenVolumeClaimsResult.isSuccessful()) {
                logger.info("JobManager and TaskManager PVC deleted")
            }
        }

        return !bootstrapExists && !jobmanagerServiceExists && !jobmanagerStatefuleSetExists && !taskmanagerStatefulSetExists && !jomanagerPVCExists && !taskmanagerPVCExists
    }
}