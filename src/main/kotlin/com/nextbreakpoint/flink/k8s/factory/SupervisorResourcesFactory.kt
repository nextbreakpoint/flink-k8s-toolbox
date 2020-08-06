package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.k8s.crd.V1SupervisorSpec
import io.kubernetes.client.openapi.models.V1Deployment

interface SupervisorResourcesFactory {
    fun createSupervisorDeployment(
        clusterSelector: ResourceSelector,
        clusterOwner: String,
        supervisor: V1SupervisorSpec,
        replicas: Int,
        dryRun: Boolean
    ): V1Deployment
}