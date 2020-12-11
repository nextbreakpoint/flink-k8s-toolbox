package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import java.util.logging.Level
import java.util.logging.Logger

class ServiceDelete(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<String, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ServiceDelete::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: String): Result<Void?> {
        return try {
            kubeClient.deleteService(namespace, params)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't delete service", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}