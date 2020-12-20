package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.crd.V1SupervisorSpec
import io.kubernetes.client.openapi.models.V1Deployment

interface SupervisorResourcesFactory {
    fun createSupervisorDeployment(
        namespace: String,
        owner: String,
        clusterName: String,
        supervisorSpec: V1SupervisorSpec,
        dryRun: Boolean
    ): V1Deployment
}