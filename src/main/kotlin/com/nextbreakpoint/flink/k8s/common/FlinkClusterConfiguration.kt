package com.nextbreakpoint.flink.k8s.common

import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster

object FlinkClusterConfiguration {
    fun getRescaleDelay(flinkCluster: V2FlinkCluster) : Long =
        flinkCluster.spec?.rescaleDelay?.toLong() ?: 60
}