package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import java.util.logging.Level
import java.util.logging.Logger

class FlinkClusterUpdate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<V1FlinkCluster, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(FlinkClusterUpdate::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: V1FlinkCluster): Result<Void?> {
        return try {
            kubeClient.updateFlinkCluster(namespace, params)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't update cluster", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}