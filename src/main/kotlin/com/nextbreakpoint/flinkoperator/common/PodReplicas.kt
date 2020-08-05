package com.nextbreakpoint.flinkoperator.common

import io.kubernetes.client.models.V1Pod

data class PodReplicas(
    val pod: V1Pod,
    val replicas: Int
)