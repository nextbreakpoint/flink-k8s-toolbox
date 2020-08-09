package com.nextbreakpoint.flinkoperator.server.supervisor.core

import com.nextbreakpoint.flinkoperator.common.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.ManualAction
import com.nextbreakpoint.flinkoperator.common.SavepointRequest
import org.apache.log4j.Logger

class TaskContext(
    private val logger: Logger,
    private val controller: TaskController
) {
    fun onTaskTimeOut() {
        logger.info("Timeout occurred")
        controller.setClusterStatus(ClusterStatus.Failed)
    }

    fun onClusterTerminated() {
        logger.info("Cluster terminated")
        controller.setClusterStatus(ClusterStatus.Terminated)
    }

    fun onClusterSuspended() {
        logger.info("Cluster suspended")
        controller.setClusterStatus(ClusterStatus.Suspended)
    }

    fun onClusterStarted() {
        logger.info("Cluster started")
        controller.setClusterStatus(ClusterStatus.Running)
    }

    fun onClusterReadyToRestart() {
        logger.info("Cluster restarted")
        controller.setClusterStatus(ClusterStatus.Updating)
    }

    fun onClusterReadyToUpdate() {
        logger.info("Resource updated")
        controller.updateStatus()
        controller.updateDigests()
        controller.setClusterStatus(ClusterStatus.Starting)
    }

    fun onClusterReadyToScale() {
        logger.info("Cluster scaled")
        controller.rescaleCluster()

        if (controller.getTaskManagers() == 0) {
            controller.setClusterStatus(ClusterStatus.Stopping)
        } else {
            controller.setClusterStatus(ClusterStatus.Starting)
        }
    }

    fun onClusterReadyToStop() {
        logger.warn("Job cancelled")
        controller.setClusterStatus(ClusterStatus.Stopping)
    }

    fun onJobFinished() {
        logger.info("Job finished")
        controller.setDeleteResources(false)
        controller.setClusterStatus(ClusterStatus.Finished)
    }

    fun onJobFailed() {
        logger.warn("Job failed")
        controller.setDeleteResources(false)
        controller.setClusterStatus(ClusterStatus.Failed)
    }

    fun onJobStopped() {
        logger.warn("Job stopped")
        controller.setClusterStatus(ClusterStatus.Restarting)
    }

    fun onResourceInitialise() {
        logger.info("Cluster initialised")
        controller.initializeAnnotations()
        controller.initializeStatus()
        controller.updateDigests()
        controller.addFinalizer()
        controller.setClusterStatus(ClusterStatus.Starting)
    }

    fun onResourceDiverged() {
        logger.info("Cluster diverged")
        controller.setClusterStatus(ClusterStatus.Restarting)
    }

    fun onResourceDeleted() {
        logger.info("Resource deleted")
        controller.setDeleteResources(true)
        controller.resetManualAction()
        controller.setClusterStatus(ClusterStatus.Stopping)
    }

    fun onResourceChanged() {
        logger.info("Resource changed")
        controller.setClusterStatus(ClusterStatus.Restarting)
    }

    fun onResourceScaled() {
        logger.info("Resource scaled")
        controller.setClusterStatus(ClusterStatus.Scaling)
    }

    fun cancelJob(): Boolean {
        val serviceExists = controller.doesServiceExists()
        val jobManagerPodExists = controller.doesJobManagerPodsExists()
        val taskManagerPodExists = controller.doesTaskManagerPodsExists()

        if (!serviceExists || !jobManagerPodExists || !taskManagerPodExists) {
            return true
        }

        val podsRunningResult = controller.arePodsRunning(controller.clusterSelector)

        if (podsRunningResult.isSuccessful() && !podsRunningResult.output) {
            return true
        }

        if (controller.isBootstrapPresent() && controller.isSavepointRequired()) {
            val savepointRequest = controller.getSavepointRequest()

            if (savepointRequest == null) {
                val cancelResult = controller.cancelJob(controller.clusterSelector, controller.getSavepointOptions())

                if (!cancelResult.isSuccessful()) {
                    logger.warn("Can't cancel the job")
                    return true
                }

                if (cancelResult.output == null) {
                    logger.info("Cancelling job...")
                    return false
                }

                if (cancelResult.output == SavepointRequest("", "")) {
                    logger.info("Job stopped without savepoint")
                    return true
                } else {
                    logger.info("Cancelling job with savepoint...")
                    controller.setSavepointRequest(cancelResult.output)
                    return false
                }
            } else {
                val querySavepointResult = controller.querySavepoint(controller.clusterSelector, savepointRequest)

                if (!querySavepointResult.isSuccessful()) {
                    logger.warn("Can't cancel job with savepoint")
                    return true
                }

                if (querySavepointResult.output != null) {
                    logger.info("Job stopped with savepoint (${querySavepointResult.output})")
                    controller.resetSavepointRequest()
                    controller.setSavepointPath(querySavepointResult.output)
                    return true
                }

                logger.info("Savepoint is in progress...")

                val seconds = controller.timeSinceLastUpdateInSeconds()

                if (seconds > Timeout.TASK_TIMEOUT) {
                    logger.error("Giving up after $seconds seconds")
                    controller.resetSavepointRequest()
                    return true
                }
            }
        } else {
            logger.info("Savepoint not required")

            val stopResult = controller.stopJob(controller.clusterSelector)

            if (!stopResult.isSuccessful()) {
                logger.warn("Can't stop the job")
                return true
            }

            if (stopResult.output) {
                logger.info("Job stopped without savepoint")
                return true
            } else {
                logger.info("Cancelling job...")
                return false
            }
        }

        return false
    }

    fun startCluster(): Boolean {
        val serviceExists = controller.doesServiceExists()
        val jobmanagerPodExists = controller.doesJobManagerPodsExists()
        val taskmanagerPodExists = controller.doesTaskManagerPodsExists()

        if (!serviceExists || !jobmanagerPodExists || !taskmanagerPodExists) {
            return false
        }

        val clusterScale = controller.getClusterScale()

        val jobmanagerReplicas = controller.getJobManagerReplicas()
        val taskmanagerReplicas = controller.getTaskManagerReplicas()

        if (jobmanagerReplicas != 1 || taskmanagerReplicas != clusterScale.taskManagers) {
            return false
        }

        if (!controller.isBootstrapPresent()) {
            val clusterReadyResult = controller.isClusterReady(controller.clusterSelector, controller.getClusterScale())

            if (!clusterReadyResult.isSuccessful() || !clusterReadyResult.output) {
                return false
            }

            logger.info("Cluster ready")

            return true
        }

        if (controller.doesBootstrapJobExists()) {
            logger.info("Cluster starting")

            val jobRunningResult = controller.isJobRunning(controller.clusterSelector)

            if (jobRunningResult.isSuccessful() && jobRunningResult.output) {
                return true
            }
        } else {
            val clusterReadyResult = controller.isClusterReady(controller.clusterSelector, controller.getClusterScale())

            if (!clusterReadyResult.isSuccessful() || !clusterReadyResult.output) {
                return false
            }

            logger.info("Cluster ready")

            val removeJarResult = controller.removeJar(controller.clusterSelector)

            if (!removeJarResult.isSuccessful()) {
                return false
            }

            logger.info("JARs removed")

            val stopResult = controller.stopJob(controller.clusterSelector)

            if (!stopResult.isSuccessful() || !stopResult.output) {
                return false
            }

            logger.info("Ready to run job")

            val bootstrapResult = controller.createBootstrapJob(controller.clusterSelector)

            if (!bootstrapResult.isSuccessful()) {
                return false
            }

            logger.info("Bootstrap job created")
        }

        return false
    }

    fun suspendCluster(): Boolean {
        val terminatedResult = controller.arePodsTerminated(controller.clusterSelector)

        if (!terminatedResult.isSuccessful() || !terminatedResult.output) {
            controller.deletePods(controller.clusterSelector, DeleteOptions(label = "role", value = "jobmanager", limit = controller.getJobManagerReplicas()))
            controller.deletePods(controller.clusterSelector, DeleteOptions(label = "role", value = "taskmanager", limit = controller.getTaskManagerReplicas()))

            return false
        }

        val bootstrapExists = controller.doesBootstrapJobExists()

        if (bootstrapExists) {
            val deleteResult = controller.deleteBootstrapJob(controller.clusterSelector)

            if (deleteResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        val serviceExists = controller.doesServiceExists()

        if (serviceExists) {
            val deleteResult = controller.deleteService(controller.clusterSelector)

            if (deleteResult.isSuccessful()) {
                logger.info("JobManager service deleted")
            }
        }

        return !serviceExists
    }

    fun terminateCluster(): Boolean {
        val terminatedResult = controller.arePodsTerminated(controller.clusterSelector)

        if (!terminatedResult.isSuccessful() || !terminatedResult.output) {
            controller.deletePods(controller.clusterSelector, DeleteOptions(label = "role", value = "jobmanager", limit = controller.getJobManagerReplicas()))
            controller.deletePods(controller.clusterSelector, DeleteOptions(label = "role", value = "taskmanager", limit = controller.getTaskManagerReplicas()))

            return false
        }

        val bootstrapExists = controller.doesBootstrapJobExists()

        if (bootstrapExists) {
            val deleteResult = controller.deleteBootstrapJob(controller.clusterSelector)

            if (deleteResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        val serviceExists = controller.doesServiceExists()

        if (serviceExists) {
            val deleteResult = controller.deleteService(controller.clusterSelector)

            if (deleteResult.isSuccessful()) {
                logger.info("JobManager service deleted")
            }
        }

        val jobmanagerPodsExists = controller.doesJobManagerPodsExists()
        val taskmanagerPodsExists = controller.doesTaskManagerPodsExists()

        if (jobmanagerPodsExists || taskmanagerPodsExists) {
            val deleteJobManagerPodsResult = controller.deletePods(controller.clusterSelector, DeleteOptions(label = "role", value = "jobmanager", limit = controller.getJobManagerReplicas()))
            val deleteTaskManagerPodsResult = controller.deletePods(controller.clusterSelector, DeleteOptions(label = "role", value = "taskmanager", limit = controller.getTaskManagerReplicas()))

            if (deleteJobManagerPodsResult.isSuccessful() && deleteTaskManagerPodsResult.isSuccessful()) {
                logger.info("JobManager and TaskManager deleted")
            }
        }

        return !serviceExists && !jobmanagerPodsExists && !taskmanagerPodsExists
    }

    fun resetCluster(): Boolean {
        val bootstrapExists = controller.doesBootstrapJobExists()

        if (bootstrapExists) {
            val bootstrapResult = controller.deleteBootstrapJob(controller.clusterSelector)

            if (bootstrapResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        return true
    }

    fun hasResourceDiverged(): Boolean {
        val serviceExists = controller.doesServiceExists()
        val jobmanagerPodExists = controller.doesJobManagerPodsExists()
        val taskmanagerPodExists = controller.doesTaskManagerPodsExists()

        if (!serviceExists || !jobmanagerPodExists || !taskmanagerPodExists) {
            return true
        }

        val clusterScale = controller.getClusterScale()

        val jobmanagerReplicas = controller.getJobManagerReplicas()
        val taskmanagerReplicas = controller.getTaskManagerReplicas()

        if (jobmanagerReplicas != 1 || taskmanagerReplicas != clusterScale.taskManagers) {
            return true
        }

        return false
    }

    fun hasResourceChanged(): Boolean {
        val changes = controller.computeChanges()

        if (changes.isNotEmpty()) {
            logger.info("Detected changes: ${changes.joinToString(separator = ",")}")
            return true
        }

        return false
    }

    fun hasJobFinished(): Boolean {
        if (controller.isBootstrapPresent()) {
            val result = controller.isJobFinished(controller.clusterSelector)

            if (result.isSuccessful() && result.output) {
                return true
            }
        }

        return false
    }

    fun hasJobFailed(): Boolean {
        if (controller.isBootstrapPresent()) {
            val result = controller.isJobFailed(controller.clusterSelector)

            if (result.isSuccessful() && result.output) {
                return true
            }
        }

        return false
    }

    fun hasJobStopped(): Boolean {
        if (controller.isBootstrapPresent()) {
            val result = controller.isJobCancelled(controller.clusterSelector)

            if (result.isSuccessful() && result.output) {
                return true
            }
        }

        return false
    }

    fun hasTaskTimedOut(): Boolean {
        val seconds = controller.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TASK_TIMEOUT) {
            logger.error("Giving up after $seconds seconds")
            return true
        }

        return false
    }

    fun hasScaleChanged(): Boolean {
        val desiredTaskManagers = controller.getDesiredTaskManagers()
        val currentTaskManagers = controller.getTaskManagers()
        return currentTaskManagers != desiredTaskManagers
    }

    fun isManualActionPresent() = controller.getManualAction() != ManualAction.NONE

    fun isResourceDeleted() = controller.hasBeenDeleted()

    fun shouldRestart() = controller.getRestartPolicy()?.toUpperCase() == "ALWAYS"

    fun mustTerminateResources() = controller.isDeleteResources()

    fun mustRecreateResources(): Boolean {
        val changes = controller.computeChanges()
        return changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")
    }

    fun removeFinalizer() {
        logger.info("Remove finalizer")
        controller.removeFinalizer()
    }

    fun ensureServiceExist(): Boolean {
        val serviceExists = controller.doesServiceExists()

        if (!serviceExists) {
            val result = controller.createService(controller.clusterSelector)

            if (result.isSuccessful()) {
                logger.info("Service created: ${result.output}")
            }

            return false
        }

        return true
    }

    fun ensurePodsExists(): Boolean {
        val clusterScale = controller.getClusterScale()

        val jobmanagerReplicas = controller.getJobManagerReplicas()
        val taskmanagerReplicas = controller.getTaskManagerReplicas()

        if (jobmanagerReplicas != 1) {
            val result = controller.createJobManagerPods(controller.clusterSelector, 1)

            if (result.isSuccessful()) {
                logger.info("JobManager created: ${result.output}")
            }

            return false
        }

        if (taskmanagerReplicas != clusterScale.taskManagers) {
            val result = controller.createTaskManagerPods(controller.clusterSelector, clusterScale.taskManagers)

            if (result.isSuccessful()) {
                logger.info("TaskManager created: ${result.output}")
            }

            return false
        }

        return true
    }

    fun executeManualAction(acceptedActions: Set<ManualAction>) {
        executeManualAction(acceptedActions, false)
    }

    fun executeManualAction(acceptedActions: Set<ManualAction>, cancelJob: Boolean) {
        logger.info("Detected manual action")

        val manualAction = controller.getManualAction()

        when (manualAction) {
            ManualAction.START -> {
                if (acceptedActions.contains(ManualAction.START)) {
                    logger.info("Start cluster")
                    controller.setClusterStatus(ClusterStatus.Starting)
                } else {
                    logger.warn("Action not allowed")
                }
            }
            ManualAction.STOP -> {
                if (acceptedActions.contains(ManualAction.STOP)) {
                    logger.info("Stop cluster")
                    if (cancelJob) {
                        controller.setClusterStatus(ClusterStatus.Cancelling)
                    } else {
                        controller.setDeleteResources(true)
                        controller.setClusterStatus(ClusterStatus.Stopping)
                    }
                } else {
                    logger.warn("Action not allowed")
                }
            }
            ManualAction.FORGET_SAVEPOINT -> {
                if (acceptedActions.contains(ManualAction.FORGET_SAVEPOINT)) {
                    logger.info("Forget savepoint path")
                    controller.setSavepointPath("")
                } else {
                    logger.warn("Action not allowed")
                }
            }
            ManualAction.TRIGGER_SAVEPOINT -> {
                if (acceptedActions.contains(ManualAction.TRIGGER_SAVEPOINT)) {
                    if (controller.isBootstrapPresent()) {
                        if (controller.getSavepointRequest() == null) {
                            val response = controller.triggerSavepoint(controller.clusterSelector, controller.getSavepointOptions())
                            if (response.isSuccessful() && response.output != null) {
                                logger.info("Savepoint requested created. Waiting for savepoint...")
                                controller.setSavepointRequest(response.output)
                            } else {
                                logger.error("Savepoint request has failed. Skipping manual savepoint")
                            }
                        } else {
                            logger.error("Savepoint request already exists. Skipping manual savepoint")
                        }
                    } else {
                        logger.info("Bootstrap not defined")
                    }
                } else {
                    logger.warn("Action not allowed")
                }
            }
            ManualAction.NONE -> {
            }
        }

        if (manualAction != ManualAction.NONE) {
            controller.resetManualAction()
        }
    }

    fun updateSavepoint() {
        if (!controller.isBootstrapPresent()) {
            return
        }

        val savepointRequest = controller.getSavepointRequest()

        if (savepointRequest != null) {
            val querySavepointResult = controller.querySavepoint(controller.clusterSelector, savepointRequest)

            if (!querySavepointResult.isSuccessful()) {
                logger.warn("Can't query savepoint")
                return
            }

            if (querySavepointResult.output != null) {
                logger.info("Savepoint created (${querySavepointResult.output})")
                controller.resetSavepointRequest()
                controller.setSavepointPath(querySavepointResult.output)
                return
            }

            logger.info("Savepoint is in progress...")

            val seconds = controller.timeSinceLastUpdateInSeconds()

            if (seconds > Timeout.TASK_TIMEOUT) {
                logger.error("Giving up after $seconds seconds")
                controller.resetSavepointRequest()
                return
            }
        } else {
            if (controller.getSavepointMode()?.toUpperCase() == "AUTOMATIC") {
                val savepointIntervalInSeconds = controller.getSavepointInterval()

                if (controller.timeSinceLastSavepointRequestInSeconds() >= savepointIntervalInSeconds) {
                    val response = controller.triggerSavepoint(controller.clusterSelector, controller.getSavepointOptions())

                    if (response.isSuccessful() && response.output != null) {
                        logger.info("Savepoint requested created. Waiting for savepoint...")
                        controller.setSavepointRequest(response.output)
                        return
                    } else {
                        logger.error("Savepoint request failed. Skipping automatic savepoint")
                        controller.resetSavepointRequest()
                        return
                    }
                }
            }
        }
    }
}

