package com.nextbreakpoint.flinkoperator.server.operator.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1Pod

data class CachedResources(
    val flinkCluster: V1FlinkCluster? = null,
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