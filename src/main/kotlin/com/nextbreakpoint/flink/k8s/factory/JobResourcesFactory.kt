package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkClusterJobSpec

interface JobResourcesFactory {
    fun createJob(
        clusterSelector: ResourceSelector,
        clusterOwner: String,
        job: V2FlinkClusterJobSpec
    ): V1FlinkJob
}