package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec

interface DeploymentResourcesFactory {
    fun createFlinkCluster(
        namespace: String,
        owner: String,
        clusterName: String,
        clusterSpec: V1FlinkClusterSpec
    ): V1FlinkCluster

    fun createFlinkJob(
        namespace: String,
        owner: String,
        clusterName: String,
        jobName: String,
        jobSpec: V1FlinkJobSpec
    ): V1FlinkJob
}