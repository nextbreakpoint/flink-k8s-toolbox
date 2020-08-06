package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.DeleteOptions
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.common.ResourceStatus
import org.apache.log4j.Logger

class ClusterManager(
    private val logger: Logger,
    private val controller: ClusterController,
    private val taskTimeout: Long = Timeout.TASK_TIMEOUT
) {
    fun removeFinalizer() {
        logger.info("Remove finalizer")
        controller.removeFinalizer()
    }

    fun onClusterTerminated() {
        logger.info("Cluster terminated")
        controller.setSupervisorStatus(ClusterStatus.Terminated)
        controller.setResourceStatus(ResourceStatus.Updated)
    }

    fun onClusterStopped() {
        logger.info("Cluster stopped")
        controller.setSupervisorStatus(ClusterStatus.Stopped)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onClusterStarted() {
        logger.info("Cluster started")
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(ClusterStatus.Started)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onClusterUnhealthy() {
        logger.info("Cluster unhealthy")
        controller.setShouldRestart(true)
        controller.setSupervisorStatus(ClusterStatus.Stopping)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onClusterReadyToRestart() {
        logger.info("Cluster restarted")
        controller.updateStatus()
        controller.updateDigests()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(ClusterStatus.Starting)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onResourceInitialise() {
        logger.info("Cluster initialised")
        controller.initializeAnnotations()
        controller.initializeStatus()
        controller.updateDigests()
        controller.addFinalizer()
        controller.setShouldRestart(false)
        controller.setSupervisorStatus(ClusterStatus.Starting)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onResourceDiverged() {
        logger.info("Cluster diverged")
        controller.setShouldRestart(true)
        controller.setSupervisorStatus(ClusterStatus.Stopping)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onResourceChanged() {
        logger.info("Resource changed")
        controller.setShouldRestart(true)
        controller.setSupervisorStatus(ClusterStatus.Stopping)
        controller.setResourceStatus(ResourceStatus.Updating)
    }

    fun onResourceDeleted() {
        logger.info("Resource deleted")
        controller.setShouldRestart(false)
        controller.setDeleteResources(true)
        controller.resetAction()
        controller.setSupervisorStatus(ClusterStatus.Stopping)
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

    fun stopCluster(): Boolean {
        val taskmanagerExist = controller.doesTaskManagerPodsExist()

        if (taskmanagerExist) {
            controller.deletePods(DeleteOptions(label = "role", value = "taskmanager", limit = controller.getTaskManagerReplicas()))
        }

        val jobmanagerExists = controller.doesJobManagerPodExists()

        if (jobmanagerExists) {
            controller.deletePods(DeleteOptions(label = "role", value = "jobmanager", limit = controller.getJobManagerReplicas()))
        }

        if (taskmanagerExist || jobmanagerExists) {
            return false
        }

        val serviceExists = controller.doesJobManagerServiceExists()

        if (serviceExists) {
            val deleteResult = controller.deleteService()

            if (deleteResult.isSuccessful()) {
                logger.info("JobManager service deleted")
            }

            return false
        }

        return true
    }

    fun hasResourceDiverged(): Boolean {
        val podExists = controller.doesJobManagerPodExists()

        val serviceExists = controller.doesJobManagerServiceExists()

        if (!serviceExists || !podExists) {
            return true
        }

        if (controller.getJobManagerReplicas() != 1) {
            return true
        }

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

    fun hasTaskTimedOut(): Boolean {
        val seconds = controller.timeSinceLastUpdateInSeconds()

        if (seconds > taskTimeout) {
            return true
        }

        return false
    }

    fun hasScaleChanged(): Boolean {
        val desiredTaskManagers = controller.getRequiredTaskManagers()
        val currentTaskManagers = controller.getCurrentTaskManagers()
        return currentTaskManagers != desiredTaskManagers
    }

    fun isActionPresent() = controller.getAction() != ManualAction.NONE

    fun isResourceDeleted() = controller.hasBeenDeleted()

    fun mustTerminateResources() = controller.isDeleteResources()

    fun mustRecreateResources(): Boolean {
        val changes = controller.computeChanges()
        return changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")
    }

    fun shouldRestart() = controller.shouldRestart()

    fun isClusterReady(): Boolean {
        val result = controller.isClusterReady()

        return result.isSuccessful() && result.output
    }

    fun isClusterUnhealthy(): Boolean {
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

    fun isClusterHealthy(): Boolean {
        val result = controller.isClusterHealthy()

        if (!result.isSuccessful() || !result.output) {
            return false
        }

        return true
    }

    fun areJobsUpdating() = controller.areJobsUpdating()

    fun ensureJobManagerServiceExists(): Boolean {
        val serviceExists = controller.doesJobManagerServiceExists()

        if (!serviceExists) {
            val result = controller.createService()

            if (result.isSuccessful()) {
                logger.info("JobManager service created")
            }

            return false
        }

        return true
    }

    fun ensureJobManagerPodExists(): Boolean {
        val jobmanagerReplicas = controller.getJobManagerReplicas()

        if (jobmanagerReplicas < 1) {
            val result = controller.createJobManagerPods(1)

            if (result.isSuccessful()) {
                logger.info("JobManager pod created")
            }

            return false
        }

        return true
    }

    fun rescaleTaskManagers(): Boolean {
        val currentTaskManagers = controller.getTaskManagers()

        val requiredTaskManagers = controller.getRequiredTaskManagers()

        if (requiredTaskManagers == currentTaskManagers) {
            return false
        }

        controller.rescaleCluster(requiredTaskManagers)

        return true
    }

    fun rescaleTaskManagerPods(): Boolean {
        val currentTaskManagers = controller.getTaskManagers()

        if (currentTaskManagers == controller.getTaskManagerReplicas()) {
            return false
        }

        if (currentTaskManagers > controller.getTaskManagerReplicas()) {
            val result = controller.createTaskManagerPods(currentTaskManagers)

            if (result.isSuccessful()) {
                logger.info("TaskManagers pods created")
            }
        } else if (controller.timeSinceLastRescaleInSeconds() > controller.getRescaleDelay()) {
            val taskmanagerIdWithPodNameMap = controller.removeUnusedTaskManagers()

            taskmanagerIdWithPodNameMap.forEach { taskmanagerIdWithPodName ->
                logger.info("TaskManager pod deleted (${taskmanagerIdWithPodName.value})")
            }
        }

        return true
    }

    fun executeAction(acceptedActions: Set<ManualAction>) {
        val manualAction = controller.getAction()

        logger.info("Detected action: $manualAction")

        when (manualAction) {
            ManualAction.START -> {
                if (acceptedActions.contains(ManualAction.START)) {
                    logger.info("Start cluster")
                    controller.updateStatus()
                    controller.updateDigests()
                    controller.setShouldRestart(true)
                    controller.setSupervisorStatus(ClusterStatus.Starting)
                    controller.setResourceStatus(ResourceStatus.Updating)
                } else {
                    logger.warn("Action not allowed")
                }
            }
            ManualAction.STOP -> {
                if (acceptedActions.contains(ManualAction.STOP)) {
                    logger.info("Stop cluster")
                    controller.setShouldRestart(false)
                    controller.setSupervisorStatus(ClusterStatus.Stopping)
                    controller.setResourceStatus(ResourceStatus.Updating)
                } else {
                    logger.warn("Action not allowed")
                }
            }
            ManualAction.NONE -> {
            }
        }

        if (manualAction != ManualAction.NONE) {
            controller.resetAction()
        }
    }

    fun stopAllJobs(): Boolean {
        val stopJobsResult = controller.stopJobs(setOf())

        if (!stopJobsResult.isSuccessful() || !stopJobsResult.output) {
            return false
        }

//        val removeJarResult = controller.removeJars()
//
//        if (!removeJarResult.isSuccessful()) {
//            logger.info("JARs not removed")
//
//            return false
//        }

        return true
    }

    fun stopUnmanagedJobs(): Boolean {
        if (controller.getJobSpecs().isEmpty()) {
            return true
        }

        val stopJobsResult = controller.stopJobs(controller.getJobIds())

        if (!stopJobsResult.isSuccessful() || !stopJobsResult.output) {
            return false
        }

        return true
    }

    fun createJobs(): Boolean {
        if (controller.haveJobsBeenCreated()) {
            return true
        }

        val jobNames = controller.listExistingJobNames()

        val results = controller.getJobSpecs().filter { job ->
            !jobNames.contains(job.name)
        }.map { job ->
            val result = controller.createJob(job)

            if (result.isSuccessful()) {
                logger.info("Job ${job.name} created")
            }

            result
        }

        if (results.any { !it.isSuccessful() }) {
            logger.error("Failed to create some jobs")
        }

        return false
    }

    fun waitForJobs(): Boolean {
        val jobs = controller.getJobNamesWithStatus()

        if (jobs.any { it.value != JobStatus.Stopped.toString() && it.value != JobStatus.Terminated.toString() }) {
            logger.warn("Wait until jobs have stopped...")

            return false
        }

        return true
    }

    fun deleteJobs(): Boolean {
        if (controller.haveJobsBeenRemoved()) {
            return true
        }

        val jobNames = controller.listExistingJobNames()

        val results = controller.getJobSpecs().filter { job ->
            jobNames.contains(job.name)
        }.map { job ->
            val result = controller.deleteJob(job.name)

            if (result.isSuccessful()) {
                logger.info("Job ${job.name} deleted")
            }

            result
        }

        if (results.any { !it.isSuccessful() }) {
            logger.error("Failed to delete some jobs")
        }

        return false
    }
}

