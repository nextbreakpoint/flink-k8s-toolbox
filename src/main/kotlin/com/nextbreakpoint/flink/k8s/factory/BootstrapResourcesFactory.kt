package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.crd.V1BootstrapSpec
import io.kubernetes.client.openapi.models.V1Job

interface BootstrapResourcesFactory {
    fun createBootstrapJob(
        namespace: String,
        owner: String,
        clusterName: String,
        jobName: String,
        bootstrapSpec: V1BootstrapSpec,
        savepointPath: String?,
        parallelism: Int,
        dryRun: Boolean
    ): V1Job
}