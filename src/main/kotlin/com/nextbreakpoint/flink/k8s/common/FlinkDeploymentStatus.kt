package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeploymentDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeploymentJobDigest
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeploymentStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobDigest
import org.joda.time.DateTime

object FlinkDeploymentStatus {
    fun getStatusTimestamp(flinkDeployment: V1FlinkDeployment) : DateTime =
        flinkDeployment.status?.timestamp ?: DateTime(0)

    fun setResourceStatus(flinkDeployment: V1FlinkDeployment, status: ResourceStatus) {
        if (flinkDeployment.status?.resourceStatus != status.toString()) {
            ensureState(flinkDeployment)

            flinkDeployment.status?.resourceStatus = status.toString()

            updateStatusTimestamp(flinkDeployment, currentTimeMillis())
        }
    }

    fun getResourceStatus(flinkDeployment: V1FlinkDeployment): ResourceStatus {
        val status = flinkDeployment.status?.resourceStatus
        return if (status.isNullOrBlank()) ResourceStatus.Unknown else ResourceStatus.valueOf(status)
    }

    fun setDeploymentDigest(flinkDeployment: V1FlinkDeployment, digest: V1FlinkDeploymentDigest) {
        ensureState(flinkDeployment)

        flinkDeployment.status?.digest = digest

        updateStatusTimestamp(flinkDeployment, currentTimeMillis())
    }

    fun getDeploymentDigest(flinkDeployment: V1FlinkDeployment): V1FlinkDeploymentDigest? =
        flinkDeployment.status?.digest

    private fun updateStatusTimestamp(flinkDeployment: V1FlinkDeployment, currentTimeMillis: Long) {
        flinkDeployment.status?.timestamp = DateTime(currentTimeMillis)
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

    private fun ensureState(flinkDeployment: V1FlinkDeployment) {
        if (flinkDeployment.status == null) {
            flinkDeployment.status = V1FlinkDeploymentStatus.builder().build()
        }
    }
}