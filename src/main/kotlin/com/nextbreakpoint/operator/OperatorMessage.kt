package com.nextbreakpoint.operator

import com.nextbreakpoint.common.model.ClusterId

data class OperatorMessage(
    val clusterId: ClusterId,
    val json: String
)