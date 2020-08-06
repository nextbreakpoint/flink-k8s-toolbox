package com.nextbreakpoint.flink.common

import io.kubernetes.client.openapi.models.V1Pod

data class PodReplicas(
    val pod: V1Pod,
    val replicas: Int
)