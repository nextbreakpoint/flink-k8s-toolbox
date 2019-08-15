package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.model.ClusterId

data class OperatorMessage(
    val clusterId: ClusterId,
    val json: String
)