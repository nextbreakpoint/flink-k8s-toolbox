package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterId

data class Command(
    val clusterId: ClusterId,
    val json: String
)