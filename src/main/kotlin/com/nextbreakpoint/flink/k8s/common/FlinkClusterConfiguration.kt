package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.common.RescalePolicy
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster

object FlinkClusterConfiguration {
    fun getRescaleDelay(flinkCluster: V1FlinkCluster) : Long =
        flinkCluster.spec?.supervisor?.rescaleDelay?.toLong() ?: 60

    fun getRescalePolicy(flinkCluster: V1FlinkCluster) : RescalePolicy {
        val rescalePolicy = flinkCluster.spec?.supervisor?.rescalePolicy
        return if (rescalePolicy.isNullOrBlank()) RescalePolicy.JobParallelism else RescalePolicy.valueOf(rescalePolicy)
    }
}