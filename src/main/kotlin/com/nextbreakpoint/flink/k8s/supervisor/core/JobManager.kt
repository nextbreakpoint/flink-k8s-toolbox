package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.common.RestartPolicy
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.common.Timeout
import org.apache.log4j.Logger

class JobManager(
    private val logger: Logger,
    private val controller: JobController,
    private val taskTimeout: Long = Timeout.TASK_TIMEOUT
) {
    fun hasFinalizer() = controller.hasFinalizer()

    fun addFinalizer() {
        logger.info("Add finalizer")
        controller.addFinalizer()
    }

    fun removeFinalizer() {
        logger.info("Remove finalizer")
        controller.removeFinalizer()
    }

    fun onClusterMissing() {
        controller.setSupervisorStatus(JobStatus.Unknown)
        controller.setResourceStatus(ResourceStatus.Updated)
    }

    fun onJobStarted() {
        logger.info("Job started")
        controller.resetSavepointRequest()
        controller.setSupervisorStatus(JobStatus.Started)
        controller.setResourceStatus(ResourceStatus.Updated)
    }

    fun onJobCanceled() {
        logger.warn("Job canceled")
        controller.resetSavepointRequest()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(JobStatus.Stopped)
        controller.setResourceStatus(ResourceStatus.Updated)
    }

    fun onJobFinished() {
        logger.info("Job finished")
        controller.resetSavepointRequest()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(JobStatus.Stopped)
        controller.setResourceStatus(ResourceStatus.Updated)
    }

    fun onJobFailed() {
        logger.warn("Job failed")
        controller.resetSavepointRequest()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(JobStatus.Stopped)
        controller.setResourceStatus(ResourceStatus.Updated)
    }

    fun onJobStopped() {
        logger.info("Job stopped")
        controller.resetJob()
        controller.resetSavepointRequest()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(JobStatus.Stopped)
        controller.setResourceStatus(ResourceStatus.Updated)
    }

    fun onJobTerminated() {
        logger.info("Job terminated")
        controller.resetSavepointRequest()
        controller.resetJob()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(JobStatus.Terminated)
        controller.setResourceStatus(ResourceStatus.Updated)
    }

    fun onJobReadyToRestart() {
        logger.info("Job restarted")
        controller.resetSavepointRequest()
        controller.updateStatus()
        controller.updateDigests()
        controller.setSupervisorStatus(JobStatus.Starting)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onJobAborted() {
        logger.info("Job aborted")
        controller.resetSavepointRequest()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(JobStatus.Stopping)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onClusterStopping() {
        logger.info("Cluster stopping")
        controller.resetSavepointRequest()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(JobStatus.Stopping)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onClusterUnhealthy() {
        logger.info("Cluster unhealthy")
        controller.resetJob()
        controller.resetSavepointRequest()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(JobStatus.Stopped)
        controller.setResourceStatus(ResourceStatus.Updated)
    }

    fun onResourceInitialise() {
        controller.initializeAnnotations()
        controller.initializeStatus()
        controller.updateDigests()
        controller.setShouldRestart(false)
        controller.resetSavepointRequest()
        controller.setClusterName(controller.clusterName)
        controller.setSupervisorStatus(JobStatus.Starting)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onResourceDeleted() {
        logger.info("Resource deleted")
        controller.resetAction()
        controller.resetSavepointRequest()
        controller.setShouldRestart(false)
        controller.setDeleteResources(true)
        controller.setSupervisorStatus(JobStatus.Stopping)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onResourceChanged() {
        logger.info("Resource changed")
        controller.resetSavepointRequest()
        controller.setShouldRestart(true)
        controller.setSupervisorStatus(JobStatus.Stopping)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun setResourceUpdated(ready: Boolean) {
        if (ready) {
            controller.setResourceStatus(ResourceStatus.Updated)
        } else {
            controller.setResourceStatus(ResourceStatus.Updating)
        }
    }

    fun setClusterHealth(health: String) {
        controller.setClusterHealth(health)
    }

    fun isClusterReady(): Boolean {
        val requireFreeSlots = controller.getRequiredTaskSlots()

        val result = controller.isClusterReady(requireFreeSlots)

        return result.isSuccessful() && result.output
    }

    fun isClusterUnhealthy(): Boolean {
        if (controller.isClusterStopping() || controller.isClusterStopped()) {
            return false
        }

        val result = controller.isClusterHealthy()

        if (result.isSuccessful() && result.output) {
            return false
        }

        val seconds = controller.timeSinceLastUpdateInSeconds()

        if (seconds > taskTimeout) {
            return true
        }

        return false
    }

    fun startJob(): Boolean {
        if (!isClusterUpdated()) {
            return false
        }

        val bootstrapJobExists = controller.doesBootstrapJobExists()

        if (!bootstrapJobExists) {
            if (controller.hasJobId()) {
                controller.resetJob()
                return false
            }

            if (controller.isWithoutSavepoint()) {
                controller.setSavepointPath("")
            }

            val result = controller.createBootstrapJob()

            if (!result.isSuccessful()) {
                logger.error("Couldn't create bootstrap job")
            }

            return false
        }

        if (!controller.hasJobId()) {
            logger.warn("Job not ready yet")
            return false
        }

        return true
    }

    fun cancelJob(): Boolean {
        if (!controller.hasJobId()) {
            return true
        }

        if (controller.shouldCreateSavepoint() && !controller.isDeleteResources() && !controller.isWithoutSavepoint()) {
            val savepointRequest = controller.getSavepointRequest()

            if (savepointRequest == null) {
                val cancelResult = controller.cancelJob(controller.getSavepointOptions())

                if (!cancelResult.isSuccessful()) {
                    logger.warn("Can't cancel the job")
                    return false
                }

                if (cancelResult.output == null) {
                    logger.info("Cancelling job...")
                    return false
                }

                if (cancelResult.output == SavepointRequest("", "")) {
                    logger.info("Job stopped without savepoint")
                    return true
                }

                logger.info("Cancelling job with savepoint...")
                controller.setSavepointRequest(cancelResult.output)
            } else {
                val querySavepointResult = controller.querySavepoint(savepointRequest)

                if (!querySavepointResult.isSuccessful()) {
                    logger.warn("Can't cancel job with savepoint")

                    if (hasTaskTimedOut()) {
                        logger.warn("Can't stop the job. Timeout occurred")
                        controller.resetJob()
                        return false
                    }

                    val stopResult = controller.stopJob()

                    if (!stopResult.isSuccessful()) {
                        logger.warn("Can't stop the job")
                        return false
                    }

                    controller.resetJob()

                    logger.info("Stopping job...")

                    return false
                }

                if (querySavepointResult.output != null) {
                    logger.info("Job stopped with savepoint (${querySavepointResult.output})")

                    controller.setSavepointPath(querySavepointResult.output)
                    controller.resetSavepointRequest()
                    controller.resetJob()

                    return true
                }

                logger.info("Cancelling job...")
            }
        } else {
            logger.info("Savepoint not required")

            if (hasTaskTimedOut()) {
                logger.warn("Can't stop the job. Timeout occurred")
                controller.resetJob()
                return false
            }

            val stopResult = controller.stopJob()

            if (!stopResult.isSuccessful()) {
                logger.warn("Can't stop the job")
                return false
            }

            controller.resetJob()

            logger.info("Stopping job...")
        }

        return false
    }

    fun stopJob(): Boolean {
        if (!controller.hasJobId()) {
            return true
        }

        if (hasTaskTimedOut()) {
            logger.warn("Can't stop the job. Timeout occurred")
            controller.resetJob()
            return false
        }

        val stopResult = controller.stopJob()

        if (!stopResult.isSuccessful()) {
            logger.warn("Can't stop the job")
            return false
        }

        controller.resetJob()

        logger.info("Stopping job...")

        return false
    }

    fun hasSpecificationChanged(): Boolean {
        val changes = controller.computeChanges()

        if (changes.isNotEmpty()) {
            logger.info("Detected changes: ${changes.joinToString(separator = ",")}")
            return true
        }

        return false
    }

    fun updateJobStatus() {
        if (controller.hasJobId()) {
            val result = controller.getJobStatus()

            if (result.isSuccessful() && result.output != null) {
                controller.setJobStatus(result.output)
            }
        }
    }

    fun isJobCancelled() = controller.isJobCancelled()

    fun isJobFinished() = controller.isJobFinished()

    fun isJobFailed() = controller.isJobFailed()

    fun hasTaskTimedOut(): Boolean {
        val seconds = controller.timeSinceLastUpdateInSeconds()

        if (seconds > taskTimeout) {
            return true
        }

        return false
    }

    fun hasParallelismChanged(): Boolean {
        val desiredJobParallelism = controller.getDeclaredJobParallelism()
        val currentJobParallelism = controller.getCurrentJobParallelism()
        return currentJobParallelism != desiredJobParallelism
    }

    fun isActionPresent() = controller.getAction() != Action.NONE

    fun isResourceDeleted() = controller.hasBeenDeleted()

    fun mustTerminateResources() = controller.isDeleteResources()

    fun shouldRestartJob() = controller.getRestartPolicy() == RestartPolicy.Always || (controller.getRestartPolicy() == RestartPolicy.OnlyIfFailed && isJobFailed())

    fun shouldRestart() = controller.shouldRestart()

    fun isClusterStopped() = controller.isClusterStopped()

    fun isClusterStopping() = controller.isClusterStopping()

    fun isClusterStarted() = controller.isClusterStarted()

    fun isClusterStarting() = controller.isClusterStarting()

    fun isClusterUpdated() = controller.isClusterUpdated()

    fun terminateBootstrapJob(): Boolean {
        val bootstrapJobExists = controller.doesBootstrapJobExists()

        if (bootstrapJobExists) {
            val deleteResult = controller.deleteBootstrapJob()

            if (deleteResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        return true
    }

    fun executeAction(acceptedActions: Set<Action>) {
        val manualAction = controller.getAction()

        logger.info("Detected action: $manualAction")

        when (manualAction) {
            Action.START -> {
                if (acceptedActions.contains(Action.START)) {
                    logger.info("Start job")
                    controller.updateStatus()
                    controller.updateDigests()
                    controller.resetSavepointRequest()
                    controller.setShouldRestart(false)
                    controller.setSupervisorStatus(JobStatus.Starting)
                    controller.setResourceStatus(ResourceStatus.Updating)
                } else {
                    logger.warn("Action not allowed")
                }
            }
            Action.STOP -> {
                if (acceptedActions.contains(Action.STOP)) {
                    logger.info("Stop job")
                    controller.resetSavepointRequest()
                    controller.setShouldRestart(false)
                    controller.setSupervisorStatus(JobStatus.Stopping)
                    controller.setResourceStatus(ResourceStatus.Updating)
                } else {
                    logger.warn("Action not allowed")
                }
            }
            Action.FORGET_SAVEPOINT -> {
                if (acceptedActions.contains(Action.FORGET_SAVEPOINT)) {
                    logger.info("Reset savepoint path")
                    controller.setSavepointPath("")
                } else {
                    logger.warn("Action not allowed")
                }
            }
            Action.TRIGGER_SAVEPOINT -> {
                if (acceptedActions.contains(Action.TRIGGER_SAVEPOINT)) {
                    if (controller.hasJobId()) {
                        if (controller.getSavepointRequest() == null) {
                            val response = controller.triggerSavepoint(controller.getSavepointOptions())
                            if (response.isSuccessful() && response.output != null) {
                                logger.info("Savepoint requested created. Waiting for savepoint...")
                                controller.setSavepointRequest(response.output)
                            } else {
                                logger.error("Savepoint request has failed. Skipping savepoint")
                            }
                        } else {
                            logger.error("Savepoint request already exists. Skipping savepoint")
                        }
                    } else {
                        logger.info("Job not started")
                    }
                } else {
                    logger.warn("Action not allowed")
                }
            }
            Action.NONE -> {
            }
        }

        if (manualAction != Action.NONE) {
            controller.resetAction()
        }
    }

    fun updateSavepoint(): Boolean {
        if (!controller.hasJobId()) {
            return true
        }

        val savepointRequest = controller.getSavepointRequest()

        if (savepointRequest != null) {
            val querySavepointResult = controller.querySavepoint(savepointRequest)

            if (!querySavepointResult.isSuccessful()) {
                logger.warn("Can't query savepoint. Savepoint aborted")
                controller.resetSavepointRequest()
                return true
            }

            if (querySavepointResult.output == null) {
                logger.info("Savepoint in progress...")
                return false
            }

            logger.info("Savepoint created (${querySavepointResult.output})")
            controller.setSavepointPath(querySavepointResult.output)
            controller.resetSavepointRequest()
        } else {
            if (controller.getSavepointInterval() > 0) {
                if (controller.timeSinceLastSavepointRequestInSeconds() >= controller.getSavepointInterval()) {
                    val triggerSavepointResult = controller.triggerSavepoint(controller.getSavepointOptions())

                    if (!triggerSavepointResult.isSuccessful()) {
                        logger.warn("Can't trigger savepoint. Savepoint aborted")
                        return true
                    }

                    if (triggerSavepointResult.output != null) {
                        logger.info("Savepoint requested created. Waiting for savepoint...")
                        controller.setSavepointRequest(triggerSavepointResult.output)
                        return false
                    }
                }
            }
        }

        return true
    }
}

