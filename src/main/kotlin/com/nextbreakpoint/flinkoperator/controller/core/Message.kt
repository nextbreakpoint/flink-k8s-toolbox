package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.model.ClusterId

data class Message(
    val clusterId: ClusterId,
    val json: String
)