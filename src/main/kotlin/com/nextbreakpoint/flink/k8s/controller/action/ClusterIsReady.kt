package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class ClusterIsReady(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<Int, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterIsReady::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: Int): Result<Boolean> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val overview = flinkClient.getOverview(address)

            Result(
                ResultStatus.OK,
                overview.slotsTotal >= params
            )
        } catch (e : Exception) {
            logger.debug("Can't get server overview")

            Result(
                ResultStatus.ERROR,
                false
            )
        }
    }
}