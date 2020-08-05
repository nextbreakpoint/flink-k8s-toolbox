package com.nextbreakpoint.flinkoperator.server.factory

import com.nextbreakpoint.flinkoperator.common.crd.V1OperatorSpec
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import io.kubernetes.client.models.V1Deployment

interface SupervisorResourcesFactory {
    fun createSupervisorDeployment(
        clusterSelector: ClusterSelector,
        clusterOwner: String,
        operator: V1OperatorSpec
    ): V1Deployment
}