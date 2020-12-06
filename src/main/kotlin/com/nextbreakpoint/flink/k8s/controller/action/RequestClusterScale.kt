package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.ScaleClusterOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class RequestClusterScale(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<ScaleClusterOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterScale::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: ScaleClusterOptions): Result<Void?> {
        return try {
            kubeClient.rescaleCluster(namespace, clusterName, params.taskManagers)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't scale cluster", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}