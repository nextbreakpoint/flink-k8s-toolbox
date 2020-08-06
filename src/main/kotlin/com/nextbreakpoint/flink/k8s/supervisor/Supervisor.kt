package com.nextbreakpoint.flink.k8s.supervisor

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterResources
import com.nextbreakpoint.flink.k8s.supervisor.core.Task
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterController
import com.nextbreakpoint.flink.k8s.supervisor.core.JobResources
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager
import com.nextbreakpoint.flink.k8s.supervisor.core.JobController
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnInitialize
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnStarted
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnStarted
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnStarting
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnStopping
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnStopped
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnTerminated
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnStopped
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnInitialise
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnStarting
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnStopping
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnTerminated
import org.apache.log4j.Logger

class Supervisor(
    private val controller: Controller,
    private val cache: Cache,
    private val taskTimeout: Long,
    private val clusterTasks: Map<ClusterStatus, Task<ClusterManager>>,
    private val jobTasks: Map<JobStatus, Task<JobManager>>
) {
    companion object {
        private val logger = Logger.getLogger(Supervisor::class.simpleName)

        fun create(controller: Controller, cache: Cache, taskTimeout: Long): Supervisor {
            val clusterTasks = mapOf(
                ClusterStatus.Unknown to ClusterOnInitialize(),
                ClusterStatus.Starting to ClusterOnStarting(),
                ClusterStatus.Stopping to ClusterOnStopping(),
                ClusterStatus.Started to ClusterOnStarted(),
                ClusterStatus.Stopped to ClusterOnStopped(),
                ClusterStatus.Terminated to ClusterOnTerminated()
            )

            val jobTasks = mapOf(
                JobStatus.Unknown to JobOnInitialise(),
                JobStatus.Starting to JobOnStarting(),
                JobStatus.Stopping to JobOnStopping(),
                JobStatus.Started to JobOnStarted(),
                JobStatus.Stopped to JobOnStopped(),
                JobStatus.Terminated to JobOnTerminated()
            )

            return create(controller, cache, taskTimeout, clusterTasks, jobTasks)
        }

        // required for testing
        fun create(controller: Controller, cache: Cache, taskTimeout: Long, clusterTasks: Map<ClusterStatus, Task<ClusterManager>>, jobTasks: Map<JobStatus, Task<JobManager>>): Supervisor {
            return Supervisor(controller, cache, taskTimeout, clusterTasks, jobTasks)
        }
    }

    private val clusterRevisions = mutableMapOf<String, State>()
    private val jobRevisions = mutableMapOf<String, State>()

    fun reconcile(clusterSelector: ResourceSelector) {
        try {
            val clusterResources = cache.getCachedClusterResources(clusterSelector)

            val cluster = clusterResources.flinkCluster ?: throw RuntimeException("Cluster not found")

            val clusterName = cluster.metadata?.labels?.get("clusterName") ?: cluster.metadata?.name.orEmpty()

            val clusterLogger = Logger.getLogger(Supervisor::class.java.name + " | Cluster: $clusterName")

            reconcile(clusterLogger, cluster, clusterResources)

            clusterResources.flinkJobs.forEach { entry ->
                val jobResources = cache.getCachedJobResources(clusterSelector, entry.key)

                val jobName = entry.value.metadata?.labels?.get("jobName") ?: entry.value.metadata?.name.orEmpty()

                val jobJogger = Logger.getLogger(Supervisor::class.java.name + " | Cluster: $clusterName | Job: $jobName")

                reconcile(jobJogger, cluster, clusterResources, jobResources)
            }
        } catch (e: Exception) {
            logger.error("Error occurred while reconciling resources", e)
        }
    }

    private fun reconcile(logger: Logger, cluster: V2FlinkCluster, clusterResources: ClusterResources) {
        val clusterSelector = ResourceSelector(
            namespace = cluster.metadata?.namespace ?: throw RuntimeException("Namespace undefined"),
            name = cluster.metadata?.name ?: throw RuntimeException("Name undefined"),
            uid = cluster.metadata?.uid ?: throw RuntimeException("Uid undefined"),
        )

        val clusterController = ClusterController(clusterSelector, cluster, clusterResources, controller)

        val actionTimestamp = clusterController.getActionTimestamp()

        val statusTimestamp = clusterController.getStatusTimestamp()

        val hasFinalizer = clusterController.hasFinalizer()

        val status = clusterController.getSupervisorStatus()

        val timestamp = controller.currentTimeMillis()

        val lastState = clusterRevisions[clusterSelector.name]

        val newState = State(status.name, cluster.metadata?.resourceVersion.orEmpty(), timestamp)

        if (lastState == null || newState.revision != lastState.revision) {
            logger.info("Resource version: ${newState.revision}")
        }

        if (lastState == null || newState.status != lastState.status) {
            logger.info("Supervisor status: ${newState.status}")
        }

        clusterRevisions[clusterSelector.name] = newState

        val clusterManager = ClusterManager(logger, clusterController, taskTimeout)

        clusterTasks[status]?.execute(clusterManager)

        clusterController.refreshStatus(logger, statusTimestamp, actionTimestamp, hasFinalizer)
    }

    private fun reconcile(logger: Logger, cluster: V2FlinkCluster, clusterResources: ClusterResources, jobResources: JobResources) {
        val job = jobResources.flinkJob ?: throw RuntimeException("Job not present")

        val clusterSelector = ResourceSelector(
            namespace = cluster.metadata?.namespace ?: throw RuntimeException("Namespace undefined"),
            name = cluster.metadata?.name ?: throw RuntimeException("Name undefined"),
            uid = cluster.metadata?.uid ?: throw RuntimeException("Uid undefined"),
        )

        val jobSelector = ResourceSelector(
            namespace = job.metadata?.namespace ?: throw RuntimeException("Namespace undefined"),
            name = job.metadata?.name ?: throw RuntimeException("Name undefined"),
            uid = job.metadata?.uid ?: throw RuntimeException("Uid undefined"),
        )

        val jobController = JobController(clusterSelector, jobSelector, cluster, job, clusterResources, jobResources, controller)

        val actionTimestamp = jobController.getActionTimestamp()

        val statusTimestamp = jobController.getStatusTimestamp()

        val hasFinalizer = jobController.hasFinalizer()

        val status = jobController.getSupervisorStatus()

        val timestamp = controller.currentTimeMillis()

        val lastState = jobRevisions[jobSelector.name]

        val newState = State(status.name, job.metadata?.resourceVersion.orEmpty(), timestamp)

        if (lastState == null || newState.revision != lastState.revision) {
            logger.info("Resource version: ${newState.revision}")
        }

        if (lastState == null || newState.status != lastState.status) {
            logger.info("Supervisor status: ${newState.status}")
        }

        jobRevisions[jobSelector.name] = newState

        val jobManager = JobManager(logger, jobController, taskTimeout)

        jobTasks[status]?.execute(jobManager)

        jobController.refreshStatus(logger, statusTimestamp, actionTimestamp, hasFinalizer)
    }

    fun cleanup() {
        val timestamp = controller.currentTimeMillis()
        clusterRevisions.entries.removeIf { timestamp - it.value.timestamp > 300000 }
        jobRevisions.entries.removeIf { timestamp - it.value.timestamp > 300000 }
    }

    private data class State(val status: String, val revision: String, val timestamp: Long)
}
