package com.nextbreakpoint.flink.vertex.core

import com.nextbreakpoint.flink.common.DeploymentSelector

data class DeploymentCommand(val selector: DeploymentSelector, val json: String)