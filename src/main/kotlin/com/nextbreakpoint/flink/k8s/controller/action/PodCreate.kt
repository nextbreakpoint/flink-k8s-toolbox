package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.models.V1Pod
import java.util.logging.Level
import java.util.logging.Logger

class PodCreate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<V1Pod, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(PodCreate::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: V1Pod): Result<String?> {
        return try {
            val podOut = kubeClient.createPod(namespace, params)

            Result(
                ResultStatus.OK,
                podOut.metadata?.name ?: throw RuntimeException("Unexpected metadata")
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't create pod", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}