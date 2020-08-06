package com.nextbreakpoint.flink.k8s.controller.core

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient

abstract class Action<T, R>(val flinkOptions: FlinkOptions, val flinkClient: FlinkClient, val kubeClient: KubeClient) {
    abstract fun execute(clusterSelector: ResourceSelector, params: T): Result<R>
}