package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.crd.V1BootstrapSpec
import com.nextbreakpoint.flink.common.ResourceSelector
import io.kubernetes.client.openapi.models.V1Job

interface BootstrapResourcesFactory {
    fun createBootstrapJob(
        clusterSelector: ResourceSelector,
        jobSelector: ResourceSelector,
        clusterOwner: String,
        jobName: String,
        bootstrap: V1BootstrapSpec,
        savepointPath: String?,
        parallelism: Int,
        dryRun: Boolean
    ): V1Job
}