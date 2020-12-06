package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.models.V1Deployment
import org.apache.log4j.Logger

class DeploymentCreate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<V1Deployment, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(DeploymentCreate::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: V1Deployment): Result<String?> {
        return try {
            val jobOut = kubeClient.createDeployment(namespace, params)

            Result(
                ResultStatus.OK,
                jobOut.metadata?.name ?: throw RuntimeException("Unexpected metadata")
            )
        } catch (e : Exception) {
            logger.error("Can't create supervisor deployment", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}