package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterDigest
import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterJobDigest
import org.joda.time.DateTime

object FlinkClusterStatus {
    fun getStatusTimestamp(flinkCluster: V2FlinkCluster) : DateTime =
        flinkCluster.status?.timestamp ?: DateTime(0)

    fun getRescaleTimestamp(flinkCluster: V2FlinkCluster) : DateTime =
        flinkCluster.status?.rescaleTimestamp ?: DateTime(0)

    fun setSupervisorStatus(flinkCluster: V2FlinkCluster, status: ClusterStatus) {
        if (flinkCluster.status?.supervisorStatus != status.toString()) {
            ensureState(flinkCluster)

            flinkCluster.status?.supervisorStatus = status.toString()

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getSupervisorStatus(flinkCluster: V2FlinkCluster) : ClusterStatus {
        val status = flinkCluster.status?.supervisorStatus
        return if (status.isNullOrBlank()) ClusterStatus.Unknown else ClusterStatus.valueOf(status)
    }

    fun setResourceStatus(flinkCluster: V2FlinkCluster, status: ResourceStatus) {
        if (flinkCluster.status?.resourceStatus != status.toString()) {
            ensureState(flinkCluster)

            flinkCluster.status?.resourceStatus = status.toString()

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getResourceStatus(flinkCluster: V2FlinkCluster): ResourceStatus {
        val status = flinkCluster.status?.resourceStatus
        return if (status.isNullOrBlank()) ResourceStatus.Unknown else ResourceStatus.valueOf(status)
    }

    fun setJobManagerDigest(flinkCluster: V2FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.jobManager = digest

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getJobManagerDigest(flinkCluster: V2FlinkCluster): String? =
        flinkCluster.status?.digest?.jobManager

    fun setTaskManagerDigest(flinkCluster: V2FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.taskManager = digest

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getTaskManagerDigest(flinkCluster: V2FlinkCluster): String? =
        flinkCluster.status?.digest?.taskManager

    fun setRuntimeDigest(flinkCluster: V2FlinkCluster, digest: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.digest?.runtime = digest

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getRuntimeDigest(flinkCluster: V2FlinkCluster): String? =
        flinkCluster.status?.digest?.runtime

    fun setJobDigests(flinkCluster: V2FlinkCluster, jobs: List<Pair<String, String>>) {
        ensureState(flinkCluster)

        flinkCluster.status?.jobs = jobs.map { makeJobDigest(it.first, it.second) }

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getJobDigests(flinkCluster: V2FlinkCluster): List<Pair<String, String>> =
        flinkCluster.status?.jobs?.map { it.name to it.digest }.orEmpty()

    fun setTaskManagers(flinkCluster: V2FlinkCluster, taskManagers: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskManagers = taskManagers

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getTaskManagers(flinkCluster: V2FlinkCluster): Int =
        flinkCluster.status?.taskManagers ?: 0

    fun setActiveTaskManagers(flinkCluster: V2FlinkCluster, taskManagers: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskManagerReplicas = taskManagers

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getActiveTaskManagers(flinkCluster: V2FlinkCluster): Int =
        flinkCluster.status?.taskManagerReplicas ?: 0

    fun setTaskSlots(flinkCluster: V2FlinkCluster, taskSlots: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.taskSlots = taskSlots

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getTaskSlots(flinkCluster: V2FlinkCluster): Int =
        flinkCluster.status?.taskSlots ?: 0

    fun setTotalTaskSlots(flinkCluster: V2FlinkCluster, totalTaskSlots: Int) {
        ensureState(flinkCluster)

        flinkCluster.status?.totalTaskSlots = totalTaskSlots

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getTotalTaskSlots(flinkCluster: V2FlinkCluster): Int =
        flinkCluster.status?.totalTaskSlots ?: 0

    fun setLabelSelector(flinkCluster: V2FlinkCluster, labelSelector: String) {
        ensureState(flinkCluster)

        flinkCluster.status?.labelSelector = labelSelector

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getLabelSelector(flinkCluster: V2FlinkCluster): String? =
        flinkCluster.status?.labelSelector

    fun setClusterHealth(flinkCluster: V2FlinkCluster, clusterHealth: String?) {
        if (flinkCluster.status?.clusterHealth != clusterHealth) {
            ensureState(flinkCluster)

            flinkCluster.status?.clusterHealth = clusterHealth

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getClusterHealth(flinkCluster: V2FlinkCluster) = flinkCluster.status?.clusterHealth

    fun setServiceMode(flinkCluster: V2FlinkCluster, serviceMode: String?) {
        ensureState(flinkCluster)

        flinkCluster.status?.serviceMode = serviceMode

        updateStatusTimestamp(flinkCluster, currentTimeMillis())
    }

    fun getServiceMode(flinkCluster: V2FlinkCluster): String? =
        flinkCluster.status?.serviceMode

    private fun updateStatusTimestamp(flinkCluster: V2FlinkCluster, currentTimeMillis: Long) {
        flinkCluster.status?.timestamp = DateTime(currentTimeMillis)
    }

    fun updateRescaleTimestamp(flinkCluster: V2FlinkCluster, currentTimeMillis: Long) {
        flinkCluster.status?.rescaleTimestamp = DateTime(currentTimeMillis)
    }

    private fun currentTimeMillis(): Long {
        // this is a hack required for testing
        ensureMillisecondPassed()

        return System.currentTimeMillis()
    }

    private fun ensureMillisecondPassed() {
        try {
            Thread.sleep(1)
        } catch (e: Exception) {
        }
    }

    private fun ensureState(flinkCluster: V2FlinkCluster) {
        if (flinkCluster.status == null) {
            flinkCluster.status = V2FlinkClusterStatus()
            flinkCluster.status.digest = V2FlinkClusterDigest()
        }
    }

    private fun makeJobDigest(name: String, digest: String): V2FlinkClusterJobDigest {
        val jobDigest = V2FlinkClusterJobDigest()
        jobDigest.name = name
        jobDigest.digest = digest
        return jobDigest
    }
}