package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.models.V1Pod
import org.apache.log4j.Logger
import java.lang.RuntimeException

class PodCreate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<V1Pod, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(PodCreate::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: V1Pod): Result<String?> {
        return try {
            val podOut = kubeClient.createPod(clusterSelector, params)

            Result(
                ResultStatus.OK,
                podOut.metadata?.name ?: throw RuntimeException("Unexpected metadata")
            )
        } catch (e : Exception) {
            logger.error("Can't create pod", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}