package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.openapi.models.V1Job

data class JobResources(
    val flinkJob: V1FlinkJob? = null,
    val bootstrapJob: V1Job? = null
) {
    fun withFlinkJob(resource: V1FlinkJob?) =
        JobResources(
            flinkJob = resource,
            bootstrapJob = this.bootstrapJob
        )

    fun withBootstrapJob(resource: V1Job?) =
        JobResources(
            flinkJob = this.flinkJob,
            bootstrapJob = resource
        )
}