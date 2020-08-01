package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import org.apache.log4j.Logger

class TaskContext(
    private val logger: Logger,
    private val mediator: TaskMediator
) {
    fun onTaskTimeOut() {
        logger.info("Timeout occurred")
        mediator.setClusterStatus(ClusterStatus.Failed)
    }

    fun onClusterTerminated() {
        logger.info("Cluster terminated")
        mediator.setClusterStatus(ClusterStatus.Terminated)
    }

    fun onClusterSuspended() {
        logger.info("Cluster suspended")
        mediator.setClusterStatus(ClusterStatus.Suspended)
    }

    fun onClusterStarted() {
        logger.info("Cluster started")
        mediator.setClusterStatus(ClusterStatus.Running)
    }

    fun onClusterReadyToRestart() {
        logger.info("Cluster restarted")
        mediator.setClusterStatus(ClusterStatus.Updating)
    }

    fun onClusterReadyToUpdate() {
        logger.info("Resource updated")
        mediator.updateStatus()
        mediator.updateDigests()
        mediator.setClusterStatus(ClusterStatus.Starting)
    }

    fun onClusterReadyToScale() {
        logger.info("Cluster scaled")
        mediator.rescaleCluster()

        if (mediator.getTaskManagers() == 0) {
            mediator.setClusterStatus(ClusterStatus.Stopping)
        } else {
            mediator.setClusterStatus(ClusterStatus.Starting)
        }
    }

    fun onClusterReadyToStop() {
        logger.warn("Job cancelled")
        mediator.setClusterStatus(ClusterStatus.Stopping)
    }

    fun onJobFinished() {
        logger.info("Job finished")
        mediator.setDeleteResources(false)
        mediator.setClusterStatus(ClusterStatus.Finished)
    }

    fun onJobFailed() {
        logger.warn("Job failed")
        mediator.setDeleteResources(false)
        mediator.setClusterStatus(ClusterStatus.Failed)
    }

    fun onResourceInitialise() {
        logger.info("Cluster initialised")
        mediator.initializeAnnotations()
        mediator.initializeStatus()
        mediator.updateDigests()
        mediator.addFinalizer()
        mediator.setClusterStatus(ClusterStatus.Starting)
    }

    fun onResourceDiverged() {
        logger.info("Cluster diverged")
        mediator.setClusterStatus(ClusterStatus.Restarting)
    }

    fun onResourceDeleted() {
        logger.info("Resource deleted")
        mediator.setDeleteResources(true)
        mediator.resetManualAction()
        mediator.setClusterStatus(ClusterStatus.Stopping)
    }

    fun onResourceChanged() {
        logger.info("Resource changed")
        mediator.setClusterStatus(ClusterStatus.Restarting)
    }

    fun onResourceScaled() {
        logger.info("Resource scaled")
        mediator.setClusterStatus(ClusterStatus.Scaling)
    }

    fun cancelJob(): Boolean {
        val serviceExists = mediator.doesServiceExists()
        val jobManagerPodExists = mediator.doesJobManagerPodsExists()
        val taskManagerPodExists = mediator.doesTaskManagerPodsExists()

        if (!serviceExists || !jobManagerPodExists || !taskManagerPodExists) {
            return true
        }

        val podsRunningResult = mediator.arePodsRunning(mediator.clusterSelector)

        if (podsRunningResult.isSuccessful() && !podsRunningResult.output) {
            return true
        }

        if (mediator.isBootstrapPresent() && mediator.isSavepointRequired()) {
            val savepointRequest = mediator.getSavepointRequest()

            if (savepointRequest == null) {
                val cancelResult = mediator.cancelJob(mediator.clusterSelector, mediator.getSavepointOptions())

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
                    mediator.setSavepointRequest(cancelResult.output)
                    return false
                }
            } else {
                val querySavepointResult = mediator.querySavepoint(mediator.clusterSelector, savepointRequest)

                if (!querySavepointResult.isSuccessful()) {
                    logger.warn("Can't create savepoint")
                    return true
                }

                if (querySavepointResult.output != null) {
                    logger.info("Job stopped with savepoint (${querySavepointResult.output})")
                    mediator.resetSavepointRequest()
                    mediator.setSavepointPath(querySavepointResult.output)
                    return true
                }

                logger.info("Savepoint is in progress...")

                val seconds = mediator.timeSinceLastUpdateInSeconds()

                if (seconds > Timeout.TASK_TIMEOUT) {
                    logger.error("Giving up after $seconds seconds")
                    mediator.resetSavepointRequest()
                    return true
                }
            }
        } else {
            logger.info("Savepoint not required")

            val stopResult = mediator.stopJob(mediator.clusterSelector)

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
        val serviceExists = mediator.doesServiceExists()
        val jobmanagerPodExists = mediator.doesJobManagerPodsExists()
        val taskmanagerPodExists = mediator.doesTaskManagerPodsExists()

        if (!serviceExists || !jobmanagerPodExists || !taskmanagerPodExists) {
            return false
        }

        val clusterScale = mediator.getClusterScale()

        val jobmanagerReplicas = mediator.getJobManagerReplicas()
        val taskmanagerReplicas = mediator.getTaskManagerReplicas()

        if (jobmanagerReplicas != 1 || taskmanagerReplicas != clusterScale.taskManagers) {
            return false
        }

        if (!mediator.isBootstrapPresent()) {
            val clusterReadyResult = mediator.isClusterReady(mediator.clusterSelector, mediator.getClusterScale())

            if (!clusterReadyResult.isSuccessful() || !clusterReadyResult.output) {
                return false
            }

            logger.info("Cluster ready")

            return true
        }

        if (mediator.doesBootstrapJobExists()) {
            logger.info("Cluster starting")

            val jobRunningResult = mediator.isJobRunning(mediator.clusterSelector)

            if (jobRunningResult.isSuccessful() && jobRunningResult.output) {
                return true
            }
        } else {
            val clusterReadyResult = mediator.isClusterReady(mediator.clusterSelector, mediator.getClusterScale())

            if (!clusterReadyResult.isSuccessful() || !clusterReadyResult.output) {
                return false
            }

            logger.info("Cluster ready")

            val removeJarResult = mediator.removeJar(mediator.clusterSelector)

            if (!removeJarResult.isSuccessful()) {
                return false
            }

            logger.info("JARs removed")

            val stopResult = mediator.stopJob(mediator.clusterSelector)

            if (!stopResult.isSuccessful() || !stopResult.output) {
                return false
            }

            logger.info("Ready to run job")

            val bootstrapResult = mediator.createBootstrapJob(mediator.clusterSelector)

            if (!bootstrapResult.isSuccessful()) {
                return false
            }

            logger.info("Bootstrap job created")
        }

        return false
    }

    fun suspendCluster(): Boolean {
        val terminatedResult = mediator.arePodsTerminated(mediator.clusterSelector)

        if (!terminatedResult.isSuccessful() || !terminatedResult.output) {
            mediator.deletePods(mediator.clusterSelector, DeleteOptions(label = "role", value = "jobmanager", limit = mediator.getJobManagerReplicas()))
            mediator.deletePods(mediator.clusterSelector, DeleteOptions(label = "role", value = "taskmanager", limit = mediator.getTaskManagerReplicas()))

            return false
        }

        val bootstrapExists = mediator.doesBootstrapJobExists()

        if (bootstrapExists) {
            val deleteResult = mediator.deleteBootstrapJob(mediator.clusterSelector)

            if (deleteResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        val serviceExists = mediator.doesServiceExists()

        if (serviceExists) {
            val deleteResult = mediator.deleteService(mediator.clusterSelector)

            if (deleteResult.isSuccessful()) {
                logger.info("JobManager service deleted")
            }
        }

        return !serviceExists
    }

    fun terminateCluster(): Boolean {
        val terminatedResult = mediator.arePodsTerminated(mediator.clusterSelector)

        if (!terminatedResult.isSuccessful() || !terminatedResult.output) {
            mediator.deletePods(mediator.clusterSelector, DeleteOptions(label = "role", value = "jobmanager", limit = mediator.getJobManagerReplicas()))
            mediator.deletePods(mediator.clusterSelector, DeleteOptions(label = "role", value = "taskmanager", limit = mediator.getTaskManagerReplicas()))

            return false
        }

        val bootstrapExists = mediator.doesBootstrapJobExists()

        if (bootstrapExists) {
            val deleteResult = mediator.deleteBootstrapJob(mediator.clusterSelector)

            if (deleteResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        val serviceExists = mediator.doesServiceExists()

        if (serviceExists) {
            val deleteResult = mediator.deleteService(mediator.clusterSelector)

            if (deleteResult.isSuccessful()) {
                logger.info("JobManager service deleted")
            }
        }

        val jobmanagerPodsExists = mediator.doesJobManagerPodsExists()
        val taskmanagerPodsExists = mediator.doesTaskManagerPodsExists()

        if (jobmanagerPodsExists || taskmanagerPodsExists) {
            val deleteJobManagerPodsResult = mediator.deletePods(mediator.clusterSelector, DeleteOptions(label = "role", value = "jobmanager", limit = mediator.getJobManagerReplicas()))
            val deleteTaskManagerPodsResult = mediator.deletePods(mediator.clusterSelector, DeleteOptions(label = "role", value = "taskmanager", limit = mediator.getTaskManagerReplicas()))

            if (deleteJobManagerPodsResult.isSuccessful() && deleteTaskManagerPodsResult.isSuccessful()) {
                logger.info("JobManager and TaskManager deleted")
            }
        }

        return !serviceExists && !jobmanagerPodsExists && !taskmanagerPodsExists
    }

    fun resetCluster(): Boolean {
        val bootstrapExists = mediator.doesBootstrapJobExists()

        if (bootstrapExists) {
            val bootstrapResult = mediator.deleteBootstrapJob(mediator.clusterSelector)

            if (bootstrapResult.isSuccessful()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        return true
    }

    fun hasResourceDiverged(): Boolean {
        val serviceExists = mediator.doesServiceExists()
        val jobmanagerPodExists = mediator.doesJobManagerPodsExists()
        val taskmanagerPodExists = mediator.doesTaskManagerPodsExists()

        if (!serviceExists || !jobmanagerPodExists || !taskmanagerPodExists) {
            return true
        }

        val clusterScale = mediator.getClusterScale()

        val jobmanagerReplicas = mediator.getJobManagerReplicas()
        val taskmanagerReplicas = mediator.getTaskManagerReplicas()

        if (jobmanagerReplicas != 1 || taskmanagerReplicas != clusterScale.taskManagers) {
            return true
        }

        return false
    }

    fun hasResourceChanged(): Boolean {
        val changes = mediator.computeChanges()

        if (changes.isNotEmpty()) {
            logger.info("Detected changes: ${changes.joinToString(separator = ",")}")
            return true
        }

        return false
    }

    fun hasJobFinished(): Boolean {
        if (mediator.isBootstrapPresent()) {
            val result = mediator.isJobFinished(mediator.clusterSelector)

            if (result.isSuccessful() && result.output) {
                return true
            }
        }

        return false
    }

    fun hasJobFailed(): Boolean {
        if (mediator.isBootstrapPresent()) {
            val result = mediator.isJobFailed(mediator.clusterSelector)

            if (result.isSuccessful() && result.output) {
                return true
            }
        }

        return false
    }

    fun hasTaskTimedOut(): Boolean {
        val seconds = mediator.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.TASK_TIMEOUT) {
            logger.error("Giving up after $seconds seconds")
            return true
        }

        return false
    }

    fun hasScaleChanged(): Boolean {
        val desiredTaskManagers = mediator.getDesiredTaskManagers()
        val currentTaskManagers = mediator.getTaskManagers()
        return currentTaskManagers != desiredTaskManagers
    }

    fun isManualActionPresent() = mediator.getManualAction() != ManualAction.NONE

    fun isResourceDeleted() = mediator.hasBeenDeleted()

    fun shouldRestart() = mediator.getRestartPolicy()?.toUpperCase() == "ALWAYS"

    fun mustTerminateResources() = mediator.isDeleteResources()

    fun mustRecreateResources(): Boolean {
        val changes = mediator.computeChanges()
        return changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")
    }

    fun removeFinalizer() {
        logger.info("Remove finalizer")
        mediator.removeFinalizer()
    }

    fun ensureServiceExist(): Boolean {
        val serviceExists = mediator.doesServiceExists()

        if (!serviceExists) {
            val result = mediator.createService(mediator.clusterSelector)

            if (result.isSuccessful()) {
                logger.info("Service created: ${result.output}")
            }

            return false
        }

        return true
    }

    fun ensurePodsExists(): Boolean {
        val clusterScale = mediator.getClusterScale()

        val jobmanagerReplicas = mediator.getJobManagerReplicas()
        val taskmanagerReplicas = mediator.getTaskManagerReplicas()

        if (jobmanagerReplicas != 1) {
            val result = mediator.createJobManagerPods(mediator.clusterSelector, 1)

            if (result.isSuccessful()) {
                logger.info("JobManager created: ${result.output}")
            }

            return false
        }

        if (taskmanagerReplicas != clusterScale.taskManagers) {
            val result = mediator.createTaskManagerPods(mediator.clusterSelector, clusterScale.taskManagers)

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

        val manualAction = mediator.getManualAction()

        when (manualAction) {
            ManualAction.START -> {
                if (acceptedActions.contains(ManualAction.START)) {
                    logger.info("Start cluster")
                    mediator.setClusterStatus(ClusterStatus.Starting)
                } else {
                    logger.warn("Action not allowed")
                }
            }
            ManualAction.STOP -> {
                if (acceptedActions.contains(ManualAction.STOP)) {
                    logger.info("Stop cluster")
                    if (cancelJob) {
                        mediator.setClusterStatus(ClusterStatus.Cancelling)
                    } else {
                        mediator.setDeleteResources(true)
                        mediator.setClusterStatus(ClusterStatus.Stopping)
                    }
                } else {
                    logger.warn("Action not allowed")
                }
            }
            ManualAction.FORGET_SAVEPOINT -> {
                if (acceptedActions.contains(ManualAction.FORGET_SAVEPOINT)) {
                    logger.info("Forget savepoint path")
                    mediator.setSavepointPath("")
                } else {
                    logger.warn("Action not allowed")
                }
            }
            ManualAction.TRIGGER_SAVEPOINT -> {
                if (acceptedActions.contains(ManualAction.TRIGGER_SAVEPOINT)) {
                    if (mediator.isBootstrapPresent()) {
                        if (mediator.getSavepointRequest() == null) {
                            val response = mediator.triggerSavepoint(mediator.clusterSelector, mediator.getSavepointOptions())
                            if (response.isSuccessful() && response.output != null) {
                                logger.info("Savepoint requested created. Waiting for savepoint...")
                                mediator.setSavepointRequest(response.output)
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
            mediator.resetManualAction()
        }
    }

    fun updateSavepoint() {
        if (!mediator.isBootstrapPresent()) {
            return
        }

        val savepointRequest = mediator.getSavepointRequest()

        if (savepointRequest != null) {
            val querySavepointResult = mediator.querySavepoint(mediator.clusterSelector, savepointRequest)

            if (!querySavepointResult.isSuccessful()) {
                logger.warn("Can't create savepoint")
                mediator.resetSavepointRequest()
                return
            }

            if (querySavepointResult.output != null) {
                logger.info("Savepoint created (${querySavepointResult.output})")
                mediator.resetSavepointRequest()
                mediator.setSavepointPath(querySavepointResult.output)
                return
            }

            logger.info("Savepoint is in progress...")

            val seconds = mediator.timeSinceLastUpdateInSeconds()

            if (seconds > Timeout.TASK_TIMEOUT) {
                logger.error("Giving up after $seconds seconds")
                mediator.resetSavepointRequest()
                return
            }
        } else {
            val savepointMode = mediator.getSavepointMode()

            if (savepointMode?.toUpperCase() == "AUTOMATIC") {
                val savepointIntervalInSeconds = mediator.getSavepointInterval()

                if (mediator.timeSinceLastSavepointRequestInSeconds() >= savepointIntervalInSeconds) {
                    val response = mediator.triggerSavepoint(mediator.clusterSelector, mediator.getSavepointOptions())

                    if (response.isSuccessful() && response.output != null) {
                        logger.info("Savepoint requested created. Waiting for savepoint...")
                        mediator.setSavepointRequest(response.output)
                        return
                    } else {
                        logger.error("Savepoint request failed. Skipping automatic savepoint")
                        mediator.resetSavepointRequest()
                        return
                    }
                }
            }
        }
    }
}

