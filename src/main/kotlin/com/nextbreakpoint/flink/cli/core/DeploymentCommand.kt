package com.nextbreakpoint.flink.cli.core

import com.nextbreakpoint.flink.common.ConnectionConfig

interface DeploymentCommand<T> {
    fun run(connectionConfig: ConnectionConfig, deploymentName: String, args: T)
}

