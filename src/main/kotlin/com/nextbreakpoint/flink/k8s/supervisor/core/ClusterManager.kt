package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.common.Timeout
import java.util.logging.Level
import java.util.logging.Logger

class ClusterManager(
    private val logger: Logger,
    private val controller: ClusterController,
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

    fun hasJobFinalizers() = controller.hasJobFinalizers()

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
            controller.deleteTaskManagers()
        }

        val jobmanagerExists = controller.doesJobManagerPodExists()

        if (jobmanagerExists) {
            controller.deleteJobManagers()
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
        if (!controller.doesJobManagerPodExists()) {
            logger.info("Detected change: JobManager pod missing")
            return true
        }

        if (!controller.doesJobManagerServiceExists()) {
            logger.info("Detected change: JobManager service missing")
            return true
        }

        if (controller.getJobManagerReplicas() != 1) {
            logger.info("Detected change: JobManager replicas")
            return true
        }

        return false
    }

    fun hasSpecificationChanged(): Boolean {
        val changes = controller.computeChanges()

        if (changes.isNotEmpty()) {
            logger.info("Detected change: ${changes.joinToString(separator = ",")}")
            return true
        }

        return false
    }

    fun hasScaleChanged(): Boolean {
        val desiredTaskManagers = controller.getClampedRequiredTaskManagers()
        val currentTaskManagers = controller.getCurrentTaskManagers()
        return currentTaskManagers != desiredTaskManagers
    }

    fun isActionPresent() = controller.getAction() != Action.NONE

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

        if (controller.timeSinceLastUpdateInSeconds() > taskTimeout) {
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

    fun areJobsReady() = controller.areJobsReady()

    fun ensureJobManagerServiceExists(): Boolean {
        val serviceExists = controller.doesJobManagerServiceExists()

        if (!serviceExists) {
            val result = controller.createService()

            if (result.isSuccessful()) {
                logger.info("JobManager service created")
            } else {
                logger.log(Level.SEVERE, "Couldn't create JobManager service")
            }
        }

        return serviceExists
    }

    fun ensureJobManagerPodExists(): Boolean {
        val jobmanagerReplicas = controller.getJobManagerReplicas()

        if (jobmanagerReplicas < 1) {
            val result = controller.createJobManagerPods(1)

            if (result.isSuccessful()) {
                logger.info("JobManager pod created")
            } else {
                logger.log(Level.SEVERE, "Couldn't create JobManager pod")
            }
        }

        return jobmanagerReplicas == 1
    }

    fun rescaleTaskManagers(): Boolean {
        val currentTaskManagers = controller.getClampedTaskManagers()

        val requiredTaskManagers = controller.getClampedRequiredTaskManagers()

        if (requiredTaskManagers == currentTaskManagers) {
            return false
        }

        logger.info("Detected change: TaskManagers")

        controller.rescaleCluster(requiredTaskManagers)

        controller.updateRescaleTimestamp()

        return true
    }

    fun rescaleTaskManagerPods(): Boolean {
        val currentTaskManagers = controller.getClampedTaskManagers()

        val taskManagerReplicas = controller.getTaskManagerReplicas()

        if (currentTaskManagers == taskManagerReplicas) {
            return false
        }

        if (currentTaskManagers > taskManagerReplicas) {
            val result = controller.createTaskManagerPods(currentTaskManagers)

            result.output.forEach { taskmanagerPodName ->
                logger.info("TaskManagers pod created ($taskmanagerPodName)")
            }

            if (result.isSuccessful()) {
                controller.updateRescaleTimestamp()
            }

            return true
        } else if (controller.timeSinceLastRescaleInSeconds() > controller.getRescaleDelay()) {
            val taskmanagerPodNames = controller.removeUnusedTaskManagers()

            taskmanagerPodNames.forEach { taskmanagerPodName ->
                logger.info("TaskManager pod deleted ($taskmanagerPodName)")
            }

            if (taskmanagerPodNames.isNotEmpty()) {
                controller.updateRescaleTimestamp()
            }

            return taskmanagerPodNames.isNotEmpty()
        }

        return false
    }

    fun executeAction(acceptedActions: Set<Action>) {
        val manualAction = controller.getAction()

        logger.info("Detected action: $manualAction")

        when (manualAction) {
            Action.START -> {
                if (acceptedActions.contains(Action.START)) {
                    logger.info("Start cluster")
                    controller.setShouldRestart(true)
                    controller.setSupervisorStatus(ClusterStatus.Stopping)
                    controller.setResourceStatus(ResourceStatus.Updating)
                } else {
                    logger.log(Level.WARNING, "Action not allowed")
                }
            }
            Action.STOP -> {
                if (acceptedActions.contains(Action.STOP)) {
                    logger.info("Stop cluster")
                    controller.setShouldRestart(false)
                    controller.setSupervisorStatus(ClusterStatus.Stopping)
                    controller.setResourceStatus(ResourceStatus.Updating)
                } else {
                    logger.log(Level.WARNING, "Action not allowed")
                }
            }
            Action.NONE -> {
            }
        }

        if (manualAction != Action.NONE) {
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
        val jobIds = controller.getJobNamesWithIds().values.filterNotNull().toSet()

        val stopJobsResult = controller.stopJobs(jobIds)

        if (!stopJobsResult.isSuccessful() || !stopJobsResult.output) {
            return false
        }

        return true
    }

    fun waitForJobs(): Boolean {
        val jobs = controller.getJobNamesWithStatus()

        if (jobs.any { it.value != JobStatus.Stopped.toString() && it.value != JobStatus.Terminated.toString() }) {
            logger.log(Level.WARNING, "Wait until jobs have stopped...")

            return false
        }

        return true
    }
}

