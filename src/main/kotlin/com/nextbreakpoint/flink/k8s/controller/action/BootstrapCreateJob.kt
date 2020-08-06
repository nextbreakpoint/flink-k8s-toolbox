package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.models.V1Job
import org.apache.log4j.Logger
import java.lang.RuntimeException

class BootstrapCreateJob(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<V1Job, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(BootstrapCreateJob::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: V1Job): Result<String?> {
        return try {
            val jobOut = kubeClient.createBootstrapJob(clusterSelector, params)

            Result(
                ResultStatus.OK,
                jobOut.metadata?.name ?: throw RuntimeException("Unexpected metadata")
            )
        } catch (e : Exception) {
            logger.error("Can't create bootstrap job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}