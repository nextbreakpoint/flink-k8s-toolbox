package com.nextbreakpoint.operator

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.model.V1FlinkCluster

data class OperatorContext(
    val lastUpdated: Long,
    val clusterId: ClusterId,
    val flinkCluster: V1FlinkCluster,
    val controller: OperatorController,
    val resources: OperatorResources
)
