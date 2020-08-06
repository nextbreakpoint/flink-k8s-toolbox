package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.JSON
import org.apache.log4j.Logger

class JobDetails(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<String, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobDetails::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: String): Result<String> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val details = flinkClient.getJobDetails(address, params)

            return Result(
                ResultStatus.OK,
                JSON().serialize(details)
            )
        } catch (e : Exception) {
            logger.error("Can't get job's details ($params)", e)

            return Result(
                ResultStatus.ERROR,
                "{}"
            )
        }
    }
}