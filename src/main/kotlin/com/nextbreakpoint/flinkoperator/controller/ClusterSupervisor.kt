package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.ClusterTask
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.TaskStatus
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Annotations
import com.nextbreakpoint.flinkoperator.controller.core.CachedResources
import com.nextbreakpoint.flinkoperator.controller.core.Configuration
import com.nextbreakpoint.flinkoperator.controller.core.OperationController
import com.nextbreakpoint.flinkoperator.controller.core.Status
import com.nextbreakpoint.flinkoperator.controller.core.TaskContext
import com.nextbreakpoint.flinkoperator.controller.core.Timeout
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultBootstrapJobFactory
import com.nextbreakpoint.flinkoperator.controller.resources.DefaultClusterResourcesFactory
import org.apache.log4j.Logger

class ClusterSupervisor(val kubeClient: KubeClient, val flinkClient: FlinkClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterSupervisor::class.simpleName)
    }

    fun reconcile(flinkOptions: FlinkOptions, namespace: String, clusterName: String) {
        val controller = OperationController(flinkOptions, flinkClient, kubeClient)

        val clusters = controller.findClusters(namespace, clusterName)

        if (clusters.items.size != 1) {
            logger.error("Expected one resource $clusterName")

            return
        }

        val cluster = clusters.items.first()

        logger.info("Resource $clusterName version: ${cluster.metadata.resourceVersion}")

        val clusterId = ClusterId(namespace = namespace, name = clusterName, uuid = cluster.metadata.uid)

        val bootstrapJobs = kubeClient.listBootstrapJobs(clusterId)
        val jobmanagerServices = kubeClient.listJobManagerServices(clusterId)
        val jobmanagerStatefulSets = kubeClient.listJobManagerStatefulSets(clusterId)
        val taskmanagerStatefulSets = kubeClient.listTaskManagerStatefulSets(clusterId)
        val jobmanagerPVCs = kubeClient.listJobManagerPVCs(clusterId)
        val taskmanagerPVCs = kubeClient.listTaskManagerPVCs(clusterId)

        val resources = CachedResources(
            bootstrapJobs = bootstrapJobs.items.map { Pair(clusterId, it) }.toMap(),
            jobmanagerServices = jobmanagerServices.items.map { Pair(clusterId, it) }.toMap(),
            jobmanagerStatefulSets = jobmanagerStatefulSets.items.map { Pair(clusterId, it) }.toMap(),
            taskmanagerStatefulSets = taskmanagerStatefulSets.items.map { Pair(clusterId, it) }.toMap(),
            jobmanagerPersistentVolumeClaims = jobmanagerPVCs.items.map { Pair(clusterId, it) }.toMap(),
            taskmanagerPersistentVolumeClaims = taskmanagerPVCs.items.map { Pair(clusterId, it) }.toMap()
        )

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

            val taskManagers = taskmanagerStatefulSets.items.firstOrNull()?.status?.readyReplicas ?: 0
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

        val jobmanagerServiceExists = context.resources.jobmanagerServices.containsKey(context.clusterId)
        val jobmanagerStatefuleSetExists = context.resources.jobmanagerStatefulSets.containsKey(context.clusterId)
        val taskmanagerStatefulSetExists = context.resources.taskmanagerStatefulSets.containsKey(context.clusterId)

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

        val jobmanagerStatefuleSetExists = context.resources.jobmanagerStatefulSets.containsKey(context.clusterId)
        val taskmanagerStatefulSetExists = context.resources.taskmanagerStatefulSets.containsKey(context.clusterId)

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

        val bootstrapExists = context.resources.bootstrapJobs.containsKey(context.clusterId)
        val jobmanagerServiceExists = context.resources.jobmanagerServices.containsKey(context.clusterId)
        val jobmanagerStatefuleSetExists = context.resources.jobmanagerStatefulSets.containsKey(context.clusterId)
        val taskmanagerStatefuleSetExists = context.resources.taskmanagerStatefulSets.containsKey(context.clusterId)

        if (!jobmanagerServiceExists) {
            val jobmanagerService = DefaultClusterResourcesFactory.createJobManagerService(
                context.clusterId.namespace, context.clusterId.uuid, "flink-operator", context.flinkCluster
            )

            val jobmanagerServiceOut = kubeClient.createService(context.clusterId, jobmanagerService)

            logger.info("JobManager service created: ${jobmanagerServiceOut.metadata.name}")
        }

        if (!jobmanagerStatefuleSetExists) {
            val jobmanagerStatefulSet = DefaultClusterResourcesFactory.createJobManagerStatefulSet(
                context.clusterId.namespace, context.clusterId.uuid, "flink-operator", context.flinkCluster
            )

            val jobmanagerStatefuleSetOut = kubeClient.createStatefulSet(context.clusterId, jobmanagerStatefulSet)

            logger.info("JobManager statefulset created: ${jobmanagerStatefuleSetOut.metadata.name}")
        }

        if (!taskmanagerStatefuleSetExists) {
            val taskmanagerStatefulSet = DefaultClusterResourcesFactory.createTaskManagerStatefulSet(
                context.clusterId.namespace, context.clusterId.uuid, "flink-operator", context.flinkCluster
            )

            val taskmanagerStatefuleSetOut = kubeClient.createStatefulSet(context.clusterId, taskmanagerStatefulSet)

            logger.info("TaskManager statefulset created: ${taskmanagerStatefuleSetOut.metadata.name}")
        }

        if (jobmanagerServiceExists && jobmanagerStatefuleSetExists && taskmanagerStatefuleSetExists) {
            val options = ClusterScaling(
                taskManagers = Status.getTaskManagers(context.flinkCluster),
                taskSlots = Status.getTaskSlots(context.flinkCluster)
            )

            val jobmanagerReplicas = context.resources.jobmanagerStatefulSets[context.clusterId]?.status?.replicas
            val taskmanagerReplicas = context.resources.taskmanagerStatefulSets[context.clusterId]?.status?.replicas

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

                val bootstrapJobOut = kubeClient.createBootstrapJob(context.clusterId, bootstrapJob)

                logger.info("Bootstrap job created: ${bootstrapJobOut.metadata.name}")

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
                kubeClient.deleteBootstrapJobs(context.clusterId)
                kubeClient.deleteBootstrapJobPods(context.clusterId)
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

        Status.appendTasks(cluster, listOf(ClusterTask.InitialiseCluster))
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
        val bootstrapExists = context.resources.bootstrapJobs.containsKey(context.clusterId)

        if (bootstrapExists) {
            kubeClient.deleteBootstrapJobs(context.clusterId)
            kubeClient.deleteBootstrapJobPods(context.clusterId)

            logger.info("Bootstrap job deleted")

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
        val bootstrapExists = context.resources.bootstrapJobs.containsKey(context.clusterId)
        val jobmanagerServiceExists = context.resources.jobmanagerServices.containsKey(context.clusterId)

        val terminatedResult = context.arePodsTerminated(context.clusterId)

        if (!terminatedResult.isCompleted()) {
            context.terminatePods(context.clusterId)

            return false
        }

        if (bootstrapExists) {
            kubeClient.deleteBootstrapJobs(context.clusterId)
            kubeClient.deleteBootstrapJobPods(context.clusterId)

            logger.info("Bootstrap job deleted")
        }

        if (jobmanagerServiceExists) {
            kubeClient.deleteJobManagerServices(context.clusterId)

            logger.info("JobManager service deleted")
        }

        return !bootstrapExists && !jobmanagerServiceExists
    }

    private fun terminate(context: TaskContext): Boolean {
        val bootstrapExists = context.resources.bootstrapJobs.containsKey(context.clusterId)
        val jobmanagerServiceExists = context.resources.jobmanagerServices.containsKey(context.clusterId)
        val jobmanagerStatefuleSetExists = context.resources.jobmanagerStatefulSets.containsKey(context.clusterId)
        val taskmanagerStatefulSetExists = context.resources.taskmanagerStatefulSets.containsKey(context.clusterId)
        val jomanagerPVCExists = context.resources.jobmanagerPersistentVolumeClaims.containsKey(context.clusterId)
        val taskmanagerPVCExists = context.resources.taskmanagerPersistentVolumeClaims.containsKey(context.clusterId)

        if (bootstrapExists) {
            kubeClient.deleteBootstrapJobs(context.clusterId)
            kubeClient.deleteBootstrapJobPods(context.clusterId)

            logger.info("Bootstrap job deleted")
        }

        if (jobmanagerServiceExists) {
            kubeClient.deleteJobManagerServices(context.clusterId)

            logger.info("JobManager service deleted")
        }

        if (jobmanagerStatefuleSetExists || taskmanagerStatefulSetExists) {
            kubeClient.deleteStatefulSets(context.clusterId)

            logger.info("JobManager and TaskManager statefulset deleted")
        }

        if (jomanagerPVCExists || taskmanagerPVCExists) {
            kubeClient.deletePersistentVolumeClaims(context.clusterId)

            logger.info("JobManager and TaskManager PVC deleted")
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
