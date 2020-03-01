package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultBootstrapJobFactory
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory
import org.apache.log4j.Logger

class ClusterSupervisor(val controller: OperationController, val clusterId: ClusterId) {
    private val logger = Logger.getLogger(ClusterSupervisor::class.simpleName + "-" + clusterId.name)

    fun reconcile() {
        try {
            val clusters = controller.findClusters(clusterId.namespace, clusterId.name)

            if (clusters.items.size != 1) {
                logger.error("Can't find cluster resource ${clusterId.name}")

                return
            }

            val cluster = clusters.items.first()

            logger.info("Resource version: ${cluster.metadata.resourceVersion}")

            val resources = controller.findClusterResources(clusterId)

            val context = TaskContext(clusterId, cluster, resources, controller)

            if (cluster.status != null) {
                val clusterStatus = Status.getClusterStatus(cluster)

                val actionTimestamp = Annotations.getActionTimestamp(cluster)

                val operatorTimestamp = Status.getOperatorTimestamp(cluster)

                logger.info("Cluster status: $clusterStatus")

                when (clusterStatus) {
                    ClusterStatus.Starting -> onStarting(context)
                    ClusterStatus.Stopping -> onStopping(context)
                    ClusterStatus.Updating -> onUpdating(context)
                    ClusterStatus.Scaling -> onScaling(context)
                    ClusterStatus.Running -> onRunning(context)
                    ClusterStatus.Failed -> onFailed(context)
                    ClusterStatus.Suspended -> onSuspended(context)
                    ClusterStatus.Terminated -> onTerminated(context)
                    ClusterStatus.Checkpointing -> onCheckpointing(context)
                    else -> {}
                }

                val taskManagers = resources.taskmanagerStatefulSet?.status?.readyReplicas ?: 0
                if (Status.getActiveTaskManagers(cluster) != taskManagers) {
                    Status.setActiveTaskManagers(cluster, taskManagers)
                }

                val taskSlots = cluster.status?.taskSlots ?: 1
                if (Status.getTotalTaskSlots(cluster) != taskManagers * taskSlots) {
                    Status.setTotalTaskSlots(cluster, taskManagers * taskSlots)
                }

                val savepointMode = cluster.spec?.operator?.savepointMode
                if (Status.getSavepointMode(cluster) != savepointMode) {
                    Status.setSavepointMode(cluster, savepointMode)
                }

                val jobRestartPolicy = cluster.spec?.operator?.jobRestartPolicy
                if (Status.getJobRestartPolicy(cluster) != jobRestartPolicy) {
                    Status.setJobRestartPolicy(cluster, jobRestartPolicy)
                }

                val newOperatorTimestamp = Status.getOperatorTimestamp(cluster)

                if (operatorTimestamp != newOperatorTimestamp) {
                    controller.updateStatus(clusterId, cluster)
                }

                val newActionTimestamp = Annotations.getActionTimestamp(cluster)

                if (actionTimestamp != newActionTimestamp) {
                    controller.updateAnnotations(clusterId, cluster)
                }

                if (hasBeenDeleted(clusterStatus, context)) {
                    doFinalize(context, controller)
                }
            } else {
                doInitialise(context, controller)
            }
        } catch (e : Exception) {
            logger.error("Error occurred while reconciling cluster ${clusterId.name}", e)
        }
    }

    private fun onFailed(context: TaskContext) {
        if (context.flinkCluster.metadata.deletionTimestamp != null) {
            Annotations.setDeleteResources(context.flinkCluster, true)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }

        val jobRunningResult = context.isJobRunning(context.clusterId)

        if (jobRunningResult.isCompleted()) {
            logger.info("Job running")

            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Running)

            return
        }

        if (Status.getJobRestartPolicy(context.flinkCluster) == "Always") {
            val changes = computeChanges(context.flinkCluster)

            if (changes.isNotEmpty()) {
                logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

                Status.setClusterStatus(context.flinkCluster, ClusterStatus.Updating)
            }
        }

        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction == ManualAction.START) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Starting)

            return
        }

        if (manualAction == ManualAction.STOP) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Annotations.setDeleteResources(context.flinkCluster, true)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }
    }

    private fun onRunning(context: TaskContext) {
        if (context.flinkCluster.metadata.deletionTimestamp != null) {
            Annotations.setDeleteResources(context.flinkCluster, true)
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }

        val jobmanagerServiceExists = context.resources.jobmanagerService != null
        val jobmanagerStatefuleSetExists = context.resources.jobmanagerStatefulSet != null
        val taskmanagerStatefulSetExists = context.resources.taskmanagerStatefulSet != null

        if (!jobmanagerServiceExists || !jobmanagerStatefuleSetExists || !taskmanagerStatefulSetExists) {
            Status.resetSavepointRequest(context.flinkCluster)
            Status.setTaskAttempts(context.flinkCluster, 0)
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Starting)

            return
        }

        val jobFinishedResult = context.isJobFinished(context.clusterId)

        if (jobFinishedResult.isCompleted()) {
            logger.info("Job has finished")

            Annotations.setDeleteResources(context.flinkCluster, false)
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }

        val jobRunningResult = context.isJobRunning(context.clusterId)

        if (!jobRunningResult.isCompleted()) {
            logger.info("Job not running")

            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Failed)

            return
        }

        val changes = computeChanges(context.flinkCluster)

        if (changes.isNotEmpty()) {
            logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Updating)

            return
        }

        val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

        if (savepointRequest != null) {
            val savepointResult = context.getLatestSavepoint(context.clusterId, savepointRequest)

            if (savepointResult.isCompleted()) {
                logger.info("Savepoint created")

                Status.resetSavepointRequest(context.flinkCluster)
                Status.setSavepointPath(context.flinkCluster, savepointResult.output)

                return
            }

            val seconds = context.timeSinceLastSavepointRequestInSeconds()

            if (seconds > Timeout.CREATING_SAVEPOINT_TIMEOUT) {
                logger.error("Savepoint not created after $seconds seconds")

                Status.resetSavepointRequest(context.flinkCluster)

                return
            }
        } else {
            val savepointMode = Status.getSavepointMode(context.flinkCluster)

            if (savepointMode?.toUpperCase() == "AUTOMATIC") {
                val savepointIntervalInSeconds = Configuration.getSavepointInterval(context.flinkCluster)

                if (context.timeSinceLastSavepointRequestInSeconds() >= savepointIntervalInSeconds) {
                    Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
                    Status.setClusterStatus(context.flinkCluster, ClusterStatus.Checkpointing)

                    return
                }
            }
        }

        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction == ManualAction.STOP) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }

        if (manualAction == ManualAction.TRIGGER_SAVEPOINT) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Checkpointing)

            return
        }

        if (manualAction == ManualAction.FORGET_SAVEPOINT) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setSavepointPath(context.flinkCluster, "")

            return
        }

        val desiredTaskManagers = context.flinkCluster.spec?.taskManagers ?: 1
        val currentTaskManagers = context.flinkCluster.status?.taskManagers ?: 1

        if (currentTaskManagers != desiredTaskManagers) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Scaling)

            return
        }
    }

    private fun onUpdating(context: TaskContext) {
        if (context.flinkCluster.metadata.deletionTimestamp != null) {
            Annotations.setDeleteResources(context.flinkCluster, true)
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }

        if (updating(context)) {
            val bootstrap = context.flinkCluster.spec?.bootstrap
            Status.setBootstrap(context.flinkCluster, bootstrap)
            val serviceMode = context.flinkCluster.spec?.jobManager?.serviceMode
            Status.setServiceMode(context.flinkCluster, serviceMode)
            val taskManagers = context.flinkCluster.spec?.taskManagers ?: 0
            val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
            Status.setTaskManagers(context.flinkCluster, taskManagers)
            Status.setTaskSlots(context.flinkCluster, taskSlots)
            Status.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Starting)
            updateDigests(context.flinkCluster)
        }
    }

    private fun onScaling(context: TaskContext) {
        if (context.flinkCluster.metadata.deletionTimestamp != null) {
            Annotations.setDeleteResources(context.flinkCluster, true)
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }

        if (cancel(context)) {
            val desiredTaskManagers = context.flinkCluster.spec?.taskManagers ?: 1
            val currentTaskSlots = context.flinkCluster.status?.taskSlots ?: 1
            Status.setTaskManagers(context.flinkCluster, desiredTaskManagers)
            Status.setTaskSlots(context.flinkCluster, currentTaskSlots)
            Status.setJobParallelism(context.flinkCluster, desiredTaskManagers * currentTaskSlots)
            if (desiredTaskManagers == 0) {
                Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)
            } else {
                Status.setClusterStatus(context.flinkCluster, ClusterStatus.Starting)
            }
        }
    }

    private fun onSuspended(context: TaskContext) {
        if (context.flinkCluster.metadata.deletionTimestamp != null) {
            Annotations.setDeleteResources(context.flinkCluster, true)
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }

        val changes = computeChanges(context.flinkCluster)

        if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")) {
            logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

            if (updating(context)) {
                val bootstrap = context.flinkCluster.spec?.bootstrap
                Status.setBootstrap(context.flinkCluster, bootstrap)
                val serviceMode = context.flinkCluster.spec?.jobManager?.serviceMode
                Status.setServiceMode(context.flinkCluster, serviceMode)
                val taskManagers = context.flinkCluster.spec?.taskManagers ?: 0
                val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
                Status.setTaskManagers(context.flinkCluster, taskManagers)
                Status.setTaskSlots(context.flinkCluster, taskSlots)
                Status.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)
                Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
                Status.setClusterStatus(context.flinkCluster, ClusterStatus.Suspended)
                updateDigests(context.flinkCluster)
            }

            return
        }

        if (!suspend(context)) {
            logger.info("Suspending cluster...")
        }

        val jobmanagerStatefuleSetExists = context.resources.jobmanagerStatefulSet != null
        val taskmanagerStatefulSetExists = context.resources.taskmanagerStatefulSet != null

        if (!jobmanagerStatefuleSetExists || !taskmanagerStatefulSetExists) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Terminated)

            return
        }

        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction == ManualAction.START) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Starting)

            return
        }

        if (manualAction == ManualAction.STOP) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }
    }

    private fun onTerminated(context: TaskContext) {
        if (context.flinkCluster.metadata.deletionTimestamp != null) {
            Annotations.setDeleteResources(context.flinkCluster, true)
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Stopping)

            return
        }

        val changes = computeChanges(context.flinkCluster)

        if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")) {
            logger.info("Detected changes: ${changes.joinToString(separator = ",")}")

            if (updating(context)) {
                val bootstrap = context.flinkCluster.spec?.bootstrap
                Status.setBootstrap(context.flinkCluster, bootstrap)
                val serviceMode = context.flinkCluster.spec?.jobManager?.serviceMode
                Status.setServiceMode(context.flinkCluster, serviceMode)
                val taskManagers = context.flinkCluster.spec?.taskManagers ?: 0
                val taskSlots = context.flinkCluster.spec?.taskManager?.taskSlots ?: 1
                Status.setTaskManagers(context.flinkCluster, taskManagers)
                Status.setTaskSlots(context.flinkCluster, taskSlots)
                Status.setJobParallelism(context.flinkCluster, taskManagers * taskSlots)
                Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
                Status.setClusterStatus(context.flinkCluster, ClusterStatus.Terminated)
                updateDigests(context.flinkCluster)
            }

            return
        }

        if (!terminate(context)) {
            logger.info("Terminating cluster...")
        }

        val manualAction = Annotations.getManualAction(context.flinkCluster)

        if (manualAction == ManualAction.START) {
            Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Starting)

            return
        }
    }

    private fun onCheckpointing(context: TaskContext) {
        val jobRunningResponse = context.isJobRunning(context.clusterId)

        if (!jobRunningResponse.isCompleted()) {
            logger.error("Job not running")

            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Failed)

            return
        }

        val options = SavepointOptions(
            targetPath = Configuration.getSavepointTargetPath(context.flinkCluster)
        )

        val response = context.triggerSavepoint(context.clusterId, options)

        if (!response.isCompleted()) {
            logger.error("Savepoint request failed")

            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Failed)

            return
        }

        logger.info("Savepoint requested created")

        Status.setSavepointRequest(context.flinkCluster, response.output)
        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Running)
    }

    private fun onStarting(context: TaskContext) {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STARTING_JOB_TIMEOUT) {
            logger.error("Cluster not started after $seconds seconds")

            Status.resetSavepointRequest(context.flinkCluster)
            Status.setTaskAttempts(context.flinkCluster, 0)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Failed)

            return
        }

        val bootstrapExists = context.resources.bootstrapJob != null
        val jobmanagerServiceExists = context.resources.jobmanagerService != null
        val jobmanagerStatefuleSetExists = context.resources.jobmanagerStatefulSet != null
        val taskmanagerStatefuleSetExists = context.resources.taskmanagerStatefulSet != null

        if (!jobmanagerServiceExists) {
            val jobmanagerService = DefaultClusterResourcesFactory.createJobManagerService(
                context.clusterId.namespace, context.clusterId.uuid, "flink-operator", context.flinkCluster
            )

            val serviceResult = context.createJobManagerService(context.clusterId, jobmanagerService)

            if (serviceResult.isCompleted()) {
                logger.info("JobManager service created: ${serviceResult.output}")
            }
        }

        if (!jobmanagerStatefuleSetExists) {
            val jobmanagerStatefulSet = DefaultClusterResourcesFactory.createJobManagerStatefulSet(
                context.clusterId.namespace, context.clusterId.uuid, "flink-operator", context.flinkCluster
            )

            val statefulSetResult = context.createStatefulSet(context.clusterId, jobmanagerStatefulSet)

            if (statefulSetResult.isCompleted()) {
                logger.info("JobManager statefulset created: ${statefulSetResult.output}")
            }
        }

        if (!taskmanagerStatefuleSetExists) {
            val taskmanagerStatefulSet = DefaultClusterResourcesFactory.createTaskManagerStatefulSet(
                context.clusterId.namespace, context.clusterId.uuid, "flink-operator", context.flinkCluster
            )

            val statefulSetResult = context.createStatefulSet(context.clusterId, taskmanagerStatefulSet)

            if (statefulSetResult.isCompleted()) {
                logger.info("TaskManager statefulset created: ${statefulSetResult.output}")
            }
        }

        if (jobmanagerServiceExists && jobmanagerStatefuleSetExists && taskmanagerStatefuleSetExists) {
            val options = ClusterScaling(
                taskManagers = Status.getTaskManagers(context.flinkCluster),
                taskSlots = Status.getTaskSlots(context.flinkCluster)
            )

            val jobmanagerReplicas = context.resources.jobmanagerStatefulSet?.status?.replicas
            val taskmanagerReplicas = context.resources.taskmanagerStatefulSet?.status?.replicas

            if (jobmanagerReplicas != 1 || taskmanagerReplicas != options.taskManagers) {
                logger.info("Restating pods...")

                context.restartPods(context.clusterId, options)

                return
            }

            if (Status.getBootstrap(context.flinkCluster) == null) {
                logger.info("Cluster running")

                Status.resetSavepointRequest(context.flinkCluster)
                Status.setTaskAttempts(context.flinkCluster, 0)
                Status.setClusterStatus(context.flinkCluster, ClusterStatus.Running)

                return
            }

            if (!bootstrapExists) {
                val clusterScale = ClusterScaling(
                    taskManagers = context.flinkCluster.status.taskManagers,
                    taskSlots = context.flinkCluster.status.taskSlots
                )

                val clusterReadyResult = context.isClusterReady(context.clusterId, clusterScale)

                if (!clusterReadyResult.isCompleted()) {
                    return
                }

                logger.info("Cluster ready")

                val removeJarResult = context.removeJar(context.clusterId)

                if (!removeJarResult.isCompleted()) {
                    return
                }

                logger.info("JARs removed")

                val savepointPath = Status.getSavepointPath(context.flinkCluster)
                val parallelism = Status.getJobParallelism(context.flinkCluster)

                val bootstrapJob = when (Annotations.isWithoutSavepoint(context.flinkCluster)) {
                    true ->
                        DefaultBootstrapJobFactory.createBootstrapJob(
                            context.clusterId, "flink-operator", context.flinkCluster.status.bootstrap, null, parallelism
                        )
                    else ->
                        DefaultBootstrapJobFactory.createBootstrapJob(
                            context.clusterId, "flink-operator", context.flinkCluster.status.bootstrap, savepointPath, parallelism
                        )
                }

                val bootstrapResult = context.createBootstrapJob(context.clusterId, bootstrapJob)

                if (bootstrapResult.isCompleted()) {
                    logger.info("Bootstrap job created: ${bootstrapResult.output}")
                }

                return
            } else {
                val jobRunningResult = context.isJobRunning(context.clusterId)

                if (!jobRunningResult.isCompleted()) {
                    return
                }

                logger.info("Job running")

                Status.resetSavepointRequest(context.flinkCluster)
                Status.setTaskAttempts(context.flinkCluster, 0)
                Status.setClusterStatus(context.flinkCluster, ClusterStatus.Running)

                return
            }
        } else {
            if (bootstrapExists) {
                val bootstrapResult = context.deleteBootstrapJob(context.clusterId)

                if (bootstrapResult.isCompleted()) {
                    logger.info("Bootstrap job deleted")
                }
            }

            return
        }
    }

    private fun onStopping(context: TaskContext) {
        val seconds = context.timeSinceLastUpdateInSeconds()

        if (seconds > Timeout.STOPPING_JOB_TIMEOUT) {
            logger.error("Cluster not stopped after $seconds seconds")

            Status.resetSavepointRequest(context.flinkCluster)
            Status.setTaskAttempts(context.flinkCluster, 0)
            Status.setClusterStatus(context.flinkCluster, ClusterStatus.Failed)

            return
        }

        if (Status.getBootstrap(context.flinkCluster) == null) {
            Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)

            return
        }

        val taskStatus = Status.getCurrentTaskStatus(context.flinkCluster)

        when (taskStatus) {
            TaskStatus.Idle -> {
                Status.setTaskStatus(context.flinkCluster, TaskStatus.Awaiting)
            }
            TaskStatus.Awaiting -> {
                if (cancel(context)) {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                }
            }
            TaskStatus.Executing -> {
                if (Annotations.isDeleteResources(context.flinkCluster)) {
                    if (terminate(context)) {
                        Status.resetSavepointRequest(context.flinkCluster)
                        Status.setTaskAttempts(context.flinkCluster, 0)
                        Status.setTaskStatus(context.flinkCluster, TaskStatus.Idle)
                        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Terminated)
                    }
                } else {
                    if (suspend(context)) {
                        Status.resetSavepointRequest(context.flinkCluster)
                        Status.setTaskAttempts(context.flinkCluster, 0)
                        Status.setTaskStatus(context.flinkCluster, TaskStatus.Idle)
                        Status.setClusterStatus(context.flinkCluster, ClusterStatus.Suspended)
                    }
                }
            }
            else -> {}
        }
    }

    private fun doInitialise(context: TaskContext, controller: OperationController) {
        val cluster = context.flinkCluster

        Status.setClusterStatus(cluster, ClusterStatus.Starting)
        Status.setTaskAttempts(cluster, 0)

        Status.setTaskStatus(cluster, TaskStatus.Idle)

        val bootstrap = cluster.spec?.bootstrap
        Status.setBootstrap(cluster, bootstrap)
        updateDigests(cluster)

        val taskManagers = cluster.spec?.taskManagers ?: 0
        val taskSlots = cluster.spec?.taskManager?.taskSlots ?: 1
        Status.setTaskManagers(cluster, taskManagers)
        Status.setTaskSlots(cluster, taskSlots)
        Status.setJobParallelism(cluster, taskManagers * taskSlots)

        val savepointPath = cluster.spec?.operator?.savepointPath
        Status.setSavepointPath(cluster, savepointPath ?: "")

        val labelSelector = ClusterResource.makeLabelSelector(context.clusterId)
        Status.setLabelSelector(cluster, labelSelector)

        val serviceMode = context.flinkCluster.spec?.jobManager?.serviceMode
        Status.setServiceMode(context.flinkCluster, serviceMode)

        val savepointMode = cluster.spec?.operator?.savepointMode
        Status.setSavepointMode(cluster, savepointMode)

        val jobRestartPolicy = cluster.spec?.operator?.jobRestartPolicy
        Status.setJobRestartPolicy(cluster, jobRestartPolicy)

        cluster.metadata.finalizers = cluster.metadata.finalizers.orEmpty().plus("finalizer.nextbreakpoint.com")

        Annotations.setDeleteResources(context.flinkCluster, false)
        Annotations.setWithoutSavepoint(context.flinkCluster, false)
        Annotations.setManualAction(context.flinkCluster, ManualAction.NONE)

        controller.updateStatus(context.clusterId, cluster)
        controller.updateFinalizers(context.clusterId, cluster)
    }

    private fun doFinalize(context: TaskContext, controller: OperationController) {
        if (context.flinkCluster.metadata.finalizers.contains("finalizer.nextbreakpoint.com")) {
            context.flinkCluster.metadata.finalizers.remove("finalizer.nextbreakpoint.com")
            controller.updateFinalizers(context.clusterId, context.flinkCluster)
        }
    }

    private fun hasBeenDeleted(clusterStatus: ClusterStatus, context: TaskContext) =
        clusterStatus == ClusterStatus.Terminated && context.flinkCluster.metadata.deletionTimestamp != null

    private fun updating(context: TaskContext): Boolean {
        val changes = computeChanges(context.flinkCluster)

        if (changes.contains("JOB_MANAGER") || changes.contains("TASK_MANAGER") || changes.contains("RUNTIME")) {
            val taskStatus = Status.getCurrentTaskStatus(context.flinkCluster)

            when (taskStatus) {
                TaskStatus.Idle -> {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Awaiting)
                }
                TaskStatus.Awaiting -> {
                    if (terminate(context)) {
                        Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                    }
                }
                TaskStatus.Executing -> {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Idle)
                    return true
                }
                else -> {}
            }
        } else if (changes.contains("BOOTSTRAP")) {
            val taskStatus = Status.getCurrentTaskStatus(context.flinkCluster)

            when (taskStatus) {
                TaskStatus.Idle -> {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Awaiting)
                }
                TaskStatus.Awaiting -> {
                    if (cancel(context)) {
                        Status.setTaskStatus(context.flinkCluster, TaskStatus.Executing)
                    }
                }
                TaskStatus.Executing -> {
                    Status.setTaskStatus(context.flinkCluster, TaskStatus.Idle)
                    return true
                }
                else -> {}
            }
        }

        return false
    }

    private fun cancel(context: TaskContext): Boolean {
        val bootstrapExists = context.resources.bootstrapJob != null

        if (bootstrapExists) {
            val bootstrapResult = context.deleteBootstrapJob(context.clusterId)

            if (bootstrapResult.isCompleted()) {
                logger.info("Bootstrap job deleted")
            }

            return false
        }

        if (Annotations.isWithoutSavepoint(context.flinkCluster) || Annotations.isDeleteResources(context.flinkCluster)) {
            val stopResult = context.stopJob(context.clusterId)

            if (stopResult.isFailed()) {
                logger.error("Failed to stop job...")

                return true
            }

            if (stopResult.isCompleted()) {
                logger.info("Stopping job without savepoint...")

                return true
            }
        } else {
            val savepointRequest = Status.getSavepointRequest(context.flinkCluster)

            if (savepointRequest == null) {
                val options = SavepointOptions(
                    targetPath = Configuration.getSavepointTargetPath(context.flinkCluster)
                )

                val cancelResult = context.cancelJob(context.clusterId, options)

                if (cancelResult.isFailed()) {
                    logger.error("Failed to cancel job...")

                    return true
                }

                if (cancelResult.isCompleted()) {
                    logger.info("Cancelling job with savepoint...")

                    Status.setSavepointRequest(context.flinkCluster, cancelResult.output)

                    return false
                }
            } else {
                val savepointResult = context.getLatestSavepoint(context.clusterId, savepointRequest)

                if (savepointResult.isFailed()) {
                    logger.error("Failed to cancel job...")

                    return true
                }

                if (savepointResult.isCompleted()) {
                    logger.info("Savepoint created")

                    Status.setSavepointPath(context.flinkCluster, savepointResult.output)
                    Status.resetSavepointRequest(context.flinkCluster)

                    return true
                }
            }
        }

        return false
    }

    private fun suspend(context: TaskContext): Boolean {
        val bootstrapExists = context.resources.bootstrapJob != null
        val jobmanagerServiceExists = context.resources.jobmanagerService != null

        val terminatedResult = context.arePodsTerminated(context.clusterId)

        if (!terminatedResult.isCompleted()) {
            context.terminatePods(context.clusterId)

            return false
        }

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

        return !bootstrapExists && !jobmanagerServiceExists
    }

    private fun terminate(context: TaskContext): Boolean {
        val bootstrapExists = context.resources.bootstrapJob != null
        val jobmanagerServiceExists = context.resources.jobmanagerService != null
        val jobmanagerStatefuleSetExists = context.resources.jobmanagerStatefulSet != null
        val taskmanagerStatefulSetExists = context.resources.taskmanagerStatefulSet != null
        val jomanagerPVCExists = context.resources.jobmanagerPVC != null
        val taskmanagerPVCExists = context.resources.taskmanagerPVC != null

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

    private fun computeChanges(flinkCluster: V1FlinkCluster): MutableList<String> {
        val jobManagerDigest = Status.getJobManagerDigest(flinkCluster)
        val taskManagerDigest = Status.getTaskManagerDigest(flinkCluster)
        val runtimeDigest = Status.getRuntimeDigest(flinkCluster)
        val bootstrapDigest = Status.getBootstrapDigest(flinkCluster)

        val actualJobManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.taskManager)
        val actualRuntimeDigest = ClusterResource.computeDigest(flinkCluster.spec?.runtime)
        val actualBootstrapDigest = ClusterResource.computeDigest(flinkCluster.spec?.bootstrap)

        val changes = mutableListOf<String>()

        if (jobManagerDigest != actualJobManagerDigest) {
            changes.add("JOB_MANAGER")
        }

        if (taskManagerDigest != actualTaskManagerDigest) {
            changes.add("TASK_MANAGER")
        }

        if (runtimeDigest != actualRuntimeDigest) {
            changes.add("RUNTIME")
        }

        if (bootstrapDigest != actualBootstrapDigest) {
            changes.add("BOOTSTRAP")
        }

        return changes
    }

    private fun updateDigests(flinkCluster: V1FlinkCluster) {
        val actualJobManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.jobManager)
        val actualTaskManagerDigest = ClusterResource.computeDigest(flinkCluster.spec?.taskManager)
        val actualRuntimeDigest = ClusterResource.computeDigest(flinkCluster.spec?.runtime)
        val actualBootstrapDigest = ClusterResource.computeDigest(flinkCluster.spec?.bootstrap)

        Status.setJobManagerDigest(flinkCluster, actualJobManagerDigest)
        Status.setTaskManagerDigest(flinkCluster, actualTaskManagerDigest)
        Status.setRuntimeDigest(flinkCluster, actualRuntimeDigest)
        Status.setBootstrapDigest(flinkCluster, actualBootstrapDigest)
    }
}
