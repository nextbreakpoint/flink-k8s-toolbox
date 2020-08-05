package com.nextbreakpoint.flinkoperator.server.controller.core

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient

abstract class Action<T, R>(val flinkOptions: FlinkOptions, val flinkClient: FlinkClient, val kubeClient: KubeClient) {
    abstract fun execute(clusterSelector: ClusterSelector, params: T): Result<R>
}