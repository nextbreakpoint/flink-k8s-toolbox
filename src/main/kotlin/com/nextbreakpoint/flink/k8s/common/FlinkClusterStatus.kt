package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterStatus
import org.joda.time.DateTime

object FlinkClusterStatus {
    fun getStatusTimestamp(flinkCluster: V1FlinkCluster) : DateTime =
        flinkCluster.status?.timestamp ?: DateTime(0)

    fun getRescaleTimestamp(flinkCluster: V1FlinkCluster) : DateTime =
        flinkCluster.status?.rescaleTimestamp ?: DateTime(0)

    fun setSupervisorStatus(flinkCluster: V1FlinkCluster, status: ClusterStatus) {
        if (flinkCluster.status?.supervisorStatus != status.toString()) {
            ensureState(flinkCluster)

            flinkCluster.status?.supervisorStatus = status.toString()

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getSupervisorStatus(flinkCluster: V1FlinkCluster) : ClusterStatus {
        val status = flinkCluster.status?.supervisorStatus
        return if (status.isNullOrBlank()) ClusterStatus.Unknown else ClusterStatus.valueOf(status)
    }

    fun setResourceStatus(flinkCluster: V1FlinkCluster, status: ResourceStatus) {
        if (flinkCluster.status?.resourceStatus != status.toString()) {
            ensureState(flinkCluster)

            flinkCluster.status?.resourceStatus = status.toString()

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getResourceStatus(flinkCluster: V1FlinkCluster): ResourceStatus {
        val status = flinkCluster.status?.resourceStatus
        return if (status.isNullOrBlank()) ResourceStatus.Unknown else ResourceStatus.valueOf(status)
    }

    fun setJobManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        if (flinkCluster.status?.digest?.jobManager != digest) {
            ensureState(flinkCluster)

            flinkCluster.status?.digest?.jobManager = digest

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getJobManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.jobManager

    fun setTaskManagerDigest(flinkCluster: V1FlinkCluster, digest: String) {
        if (flinkCluster.status?.digest?.taskManager != digest) {
            ensureState(flinkCluster)

            flinkCluster.status?.digest?.taskManager = digest

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getTaskManagerDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.taskManager

    fun setRuntimeDigest(flinkCluster: V1FlinkCluster, digest: String) {
        if (flinkCluster.status?.digest?.runtime != digest) {
            ensureState(flinkCluster)

            flinkCluster.status?.digest?.runtime = digest

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getRuntimeDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.runtime

    fun setSupervisorDigest(flinkCluster: V1FlinkCluster, digest: String) {
        if (flinkCluster.status?.digest?.supervisor != digest) {
            ensureState(flinkCluster)

            flinkCluster.status?.digest?.supervisor = digest

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getSupervisorDigest(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.digest?.supervisor

    fun setTaskManagers(flinkCluster: V1FlinkCluster, taskManagers: Int) {
        if (flinkCluster.status?.taskManagers != taskManagers) {
            ensureState(flinkCluster)

            flinkCluster.status?.taskManagers = taskManagers

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getTaskManagers(flinkCluster: V1FlinkCluster): Int =
        flinkCluster.status?.taskManagers ?: 0

    fun setTaskManagerReplicas(flinkCluster: V1FlinkCluster, replicas: Int) {
        if (flinkCluster.status?.taskManagerReplicas != replicas) {
            ensureState(flinkCluster)

            flinkCluster.status?.taskManagerReplicas = replicas

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getTaskManagerReplicas(flinkCluster: V1FlinkCluster): Int? =
        flinkCluster.status?.taskManagerReplicas

    fun setTaskSlots(flinkCluster: V1FlinkCluster, taskSlots: Int) {
        if (flinkCluster.status?.taskSlots != taskSlots) {
            ensureState(flinkCluster)

            flinkCluster.status?.taskSlots = taskSlots

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getTaskSlots(flinkCluster: V1FlinkCluster): Int? =
        flinkCluster.status?.taskSlots

    fun setTotalTaskSlots(flinkCluster: V1FlinkCluster, totalTaskSlots: Int) {
        if (flinkCluster.status?.totalTaskSlots != totalTaskSlots) {
            ensureState(flinkCluster)

            flinkCluster.status?.totalTaskSlots = totalTaskSlots

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getTotalTaskSlots(flinkCluster: V1FlinkCluster): Int? =
        flinkCluster.status?.totalTaskSlots

    fun setLabelSelector(flinkCluster: V1FlinkCluster, labelSelector: String) {
        if (flinkCluster.status?.labelSelector != labelSelector) {
            ensureState(flinkCluster)

            flinkCluster.status?.labelSelector = labelSelector

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getLabelSelector(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.labelSelector

    fun setClusterHealth(flinkCluster: V1FlinkCluster, clusterHealth: String?) {
        if (flinkCluster.status?.clusterHealth != clusterHealth) {
            ensureState(flinkCluster)

            flinkCluster.status?.clusterHealth = clusterHealth

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getClusterHealth(flinkCluster: V1FlinkCluster) = flinkCluster.status?.clusterHealth

    fun setServiceMode(flinkCluster: V1FlinkCluster, serviceMode: String?) {
        if (flinkCluster.status?.serviceMode != serviceMode) {
            ensureState(flinkCluster)

            flinkCluster.status?.serviceMode = serviceMode

            updateStatusTimestamp(flinkCluster, currentTimeMillis())
        }
    }

    fun getServiceMode(flinkCluster: V1FlinkCluster): String? =
        flinkCluster.status?.serviceMode

    private fun updateStatusTimestamp(flinkCluster: V1FlinkCluster, currentTimeMillis: Long) {
        flinkCluster.status?.timestamp = DateTime(currentTimeMillis)
    }

    fun updateRescaleTimestamp(flinkCluster: V1FlinkCluster, currentTimeMillis: Long) {
        flinkCluster.status?.rescaleTimestamp = DateTime(currentTimeMillis)
    }

    @JvmStatic
    private fun currentTimeMillis(): Long {
        // this is a hack required for testing
        ensureMillisecondPassed()

        return System.currentTimeMillis()
    }

    @JvmStatic
    private fun ensureMillisecondPassed() {
        try {
            Thread.sleep(1)
        } catch (e: Exception) {
        }
    }

    private fun ensureState(flinkCluster: V1FlinkCluster) {
        if (flinkCluster.status == null) {
            flinkCluster.status = V1FlinkClusterStatus.builder().build()
            flinkCluster.status.digest = V1FlinkClusterDigest.builder().build()
        }
    }
}