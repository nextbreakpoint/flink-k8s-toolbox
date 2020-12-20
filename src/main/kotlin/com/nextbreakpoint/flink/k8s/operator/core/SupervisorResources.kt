package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod

data class SupervisorResources(
    val flinkCluster: V1FlinkCluster? = null,
    val supervisorDep: V1Deployment? = null,
    val supervisorPods: Set<V1Pod>
)