package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.openapi.models.V1Job

data class JobResources(
    val flinkJob: V1FlinkJob? = null,
    val bootstrapJob: V1Job? = null
) {
    fun withBootstrap(resource: V1Job?) =
        JobResources(
            flinkJob = this.flinkJob,
            bootstrapJob = resource
        )
}