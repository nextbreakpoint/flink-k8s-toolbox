package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod

data class CachedResources(
    val flinkCluster: V2FlinkCluster? = null,
    val supervisorDeployment: V1Deployment? = null,
    val supervisorPod: V1Pod? = null
) {
    fun withSupervisorPod(resource: V1Pod?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            supervisorDeployment = this.supervisorDeployment,
            supervisorPod = resource
        )

    fun withSupervisorDeployment(resource: V1Deployment?) =
        CachedResources(
            flinkCluster = this.flinkCluster,
            supervisorDeployment = resource,
            supervisorPod = this.supervisorPod
        )
}