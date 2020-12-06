package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterStatus

class FlinkClusterGetStatus(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: ClusterContext) : ClusterAction<Void?, V1FlinkClusterStatus?>(flinkOptions, flinkClient, kubeClient) {
    override fun execute(namespace: String, clusterName: String, params: Void?): Result<V1FlinkClusterStatus?> {
        return Result(ResultStatus.OK, context.getClusterStatus())
    }
}