package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class BootstrapDeleteJob(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<String, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(BootstrapDeleteJob::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: String): Result<Void?> {
        return try {
            kubeClient.deleteBootstrapJob(clusterSelector, params)

            kubeClient.deleteBootstrapPod(clusterSelector, params)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't delete bootstrap job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}