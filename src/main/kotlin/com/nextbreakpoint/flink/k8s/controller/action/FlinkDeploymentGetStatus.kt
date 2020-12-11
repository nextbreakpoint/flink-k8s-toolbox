package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.DeploymentContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeploymentStatus

class FlinkDeploymentGetStatus(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: DeploymentContext) : ClusterAction<Void?, V1FlinkDeploymentStatus?>(flinkOptions, flinkClient, kubeClient) {
    override fun execute(namespace: String, clusterName: String, params: Void?): Result<V1FlinkDeploymentStatus?> {
        return Result(ResultStatus.OK, context.getDeploymentStatus())
    }
}