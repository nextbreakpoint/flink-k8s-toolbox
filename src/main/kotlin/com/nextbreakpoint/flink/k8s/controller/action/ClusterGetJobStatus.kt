package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class ClusterGetJobStatus(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<String, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobGetStatus::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: String): Result<String?> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val jobDetails = flinkClient.getJobDetails(address, params)

            Result(
                ResultStatus.OK,
                jobDetails.state.value.toString().toUpperCase()
            )
        } catch (e: Exception) {
            logger.warn("Can't get job details")

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}
