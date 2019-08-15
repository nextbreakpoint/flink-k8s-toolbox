package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster

data class OperatorContext(
    val lastUpdated: Long,
    val clusterId: ClusterId,
    val flinkCluster: V1FlinkCluster,
    val controller: OperatorController,
    val resources: OperatorResources
)
