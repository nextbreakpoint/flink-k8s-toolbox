package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.models.V1Service
import org.apache.log4j.Logger

class ServiceCreate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<V1Service, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ServiceCreate::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: V1Service): Result<String?> {
        return try {
            val serviceOut = kubeClient.createService(namespace, params)

            Result(
                ResultStatus.OK,
                serviceOut.metadata?.name ?: throw RuntimeException("Unexpected metadata")
            )
        } catch (e : Exception) {
            logger.error("Can't create service", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}