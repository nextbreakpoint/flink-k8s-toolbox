package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class FlinkClusterCreate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<V2FlinkCluster, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(FlinkClusterCreate::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: V2FlinkCluster): Result<Void?> {
        return try {
            val flinkCluster = V2FlinkCluster()
                .apiVersion("nextbreakpoint.com/v2")
                .kind("FlinkCluster")
                .metadata(params.metadata)
                .spec(params.spec)

            kubeClient.createFlinkClusterV2(flinkCluster)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't create cluster resource", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}