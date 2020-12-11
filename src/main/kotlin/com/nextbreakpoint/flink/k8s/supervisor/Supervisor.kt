package com.nextbreakpoint.flink.k8s.supervisor

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ServerConfig
import com.nextbreakpoint.flink.k8s.common.Task
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterController
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterManager
import com.nextbreakpoint.flink.k8s.supervisor.core.ClusterResources
import com.nextbreakpoint.flink.k8s.supervisor.core.JobController
import com.nextbreakpoint.flink.k8s.supervisor.core.JobManager
import com.nextbreakpoint.flink.k8s.supervisor.core.JobResources
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnInitialize
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnStarted
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnStarting
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnStopped
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnStopping
import com.nextbreakpoint.flink.k8s.supervisor.task.ClusterOnTerminated
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnInitialise
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnStarted
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnStarting
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnStopped
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnStopping
import com.nextbreakpoint.flink.k8s.supervisor.task.JobOnTerminated
import java.util.logging.Level
import java.util.logging.Logger

class Supervisor(
    private val controller: Controller,
    private val cache: Cache,
    private val taskTimeout: Long,
    private val pollingInterval: Long,
    private val serverConfig: ServerConfig,
    private val clusterTasks: Map<ClusterStatus, Task<ClusterManager>>,
    private val jobTasks: Map<JobStatus, Task<JobManager>>
) {
    companion object {
        private val logger = Logger.getLogger(Supervisor::class.simpleName)

        fun create(controller: Controller, cache: Cache, taskTimeout: Long, pollingInterval: Long, serverConfig: ServerConfig): Supervisor {
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

            return create(controller, cache, taskTimeout, pollingInterval, serverConfig, clusterTasks, jobTasks)
        }

        // required for testing
        fun create(controller: Controller, cache: Cache, taskTimeout: Long, pollingInterval: Long, serverConfig: ServerConfig, clusterTasks: Map<ClusterStatus, Task<ClusterManager>>, jobTasks: Map<JobStatus, Task<JobManager>>): Supervisor {
            return Supervisor(controller, cache, taskTimeout, pollingInterval, serverConfig, clusterTasks, jobTasks)
        }
    }

    private val clusterRevisions = mutableMapOf<String, State>()
    private val jobRevisions = mutableMapOf<String, State>()

    fun reconcile() {
        try {
            val clusterName = cache.clusterName

            val clusterResources = cache.getClusterResources()

            if (clusterResources.flinkCluster != null) {
                val logger = Logger.getLogger(getLoggerName(clusterName, null))

                reconcile(logger, clusterName, clusterResources)
            }

            clusterResources.flinkJobs.forEach { job ->
                val jobName = job.metadata?.name?.removePrefix("$clusterName-") ?: throw RuntimeException("Metadata name is null")

                val jobResources = cache.getJobResources(jobName)

                val logger = Logger.getLogger(getLoggerName(clusterName, jobName))

                reconcile(logger, clusterName, jobName, clusterResources, jobResources)
            }
        } catch (e: Exception) {
            logger.log(Level.SEVERE, "Error occurred while reconciling resources", e)
        }
    }

    private fun reconcile(logger: Logger, clusterName: String, clusterResources: ClusterResources) {
        val cluster = clusterResources.flinkCluster ?: throw RuntimeException("Cluster not found")

        val clusterController = ClusterController(cache.namespace, clusterName, controller, pollingInterval, clusterResources, cluster)

        val actionTimestamp = clusterController.getActionTimestamp()

        val statusTimestamp = clusterController.getStatusTimestamp()

        val hasFinalizer = clusterController.hasFinalizer()

        val status = clusterController.getSupervisorStatus()

        val timestamp = controller.currentTimeMillis()

        val lastState = clusterRevisions[clusterName]

        val newState = State(status.name, cluster.metadata?.resourceVersion.orEmpty(), timestamp)

        if (lastState == null || newState.revision != lastState.revision) {
            logger.info("Resource version: ${newState.revision}")
        }

        if (lastState == null || newState.status != lastState.status) {
            logger.info("Supervisor status: ${newState.status}")
        }

        clusterRevisions[clusterName] = newState

        val clusterManager = ClusterManager(logger, clusterController, taskTimeout)

        clusterTasks[status]?.execute(clusterManager)

        clusterController.refreshStatus(logger, statusTimestamp, actionTimestamp, hasFinalizer)
    }

    private fun reconcile(logger: Logger, clusterName: String, jobName: String, clusterResources: ClusterResources, jobResources: JobResources) {
        val job = jobResources.flinkJob ?: throw RuntimeException("Job not found")

        val jobController = JobController(cache.namespace, clusterName, jobName, controller, pollingInterval, clusterResources, jobResources, job)

        val actionTimestamp = jobController.getActionTimestamp()

        val statusTimestamp = jobController.getStatusTimestamp()

        val hasFinalizer = jobController.hasFinalizer()

        val status = jobController.getSupervisorStatus()

        val timestamp = controller.currentTimeMillis()

        val lastState = jobRevisions["$clusterName-$jobName"]

        val newState = State(status.name, job.metadata?.resourceVersion.orEmpty(), timestamp)

        if (lastState == null || newState.revision != lastState.revision) {
            logger.info("Resource version: ${newState.revision}")
        }

        if (lastState == null || newState.status != lastState.status) {
            logger.info("Supervisor status: ${newState.status}")
        }

        jobRevisions["$clusterName-$jobName"] = newState

        val jobManager = JobManager(logger, jobController, taskTimeout)

        if (clusterResources.flinkCluster != null) {
            jobTasks[status]?.execute(jobManager)
        } else {
            jobManager.onClusterMissing()
        }

        jobController.refreshStatus(logger, statusTimestamp, actionTimestamp, hasFinalizer)
    }

    fun cleanup() {
        val timestamp = controller.currentTimeMillis()
        clusterRevisions.entries.removeIf { timestamp - it.value.timestamp > 300000 }
        jobRevisions.entries.removeIf { timestamp - it.value.timestamp > 300000 }
    }

    private fun getLoggerName(clusterName: String, jobName: String?) =
        Supervisor::class.simpleName + " ${jobName?.let { "$clusterName-$jobName" } ?: clusterName}"

    private data class State(val status: String, val revision: String, val timestamp: Long)
}
