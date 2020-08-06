package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.SavepointOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class JobCancel(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: JobContext) : Action<SavepointOptions, SavepointRequest?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobCancel::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: SavepointOptions): Result<SavepointRequest?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val requests = flinkClient.cancelJobs(address, listOf(context.getJobId()), params.targetPath)

            return if (requests.isEmpty()) {
                flinkClient.terminateJobs(address, listOf(context.getJobId()))

                Result(
                    ResultStatus.OK,
                    null
                )
            } else {
                Result(
                    ResultStatus.OK,
                    requests.map { SavepointRequest(jobId = it.key, triggerId = it.value) }.firstOrNull()
                )
            }
        } catch (e : Exception) {
            logger.error("Can't cancel job (${context.getJobId()})", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}