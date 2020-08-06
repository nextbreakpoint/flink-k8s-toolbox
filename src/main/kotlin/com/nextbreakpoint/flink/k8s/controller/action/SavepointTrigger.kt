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

class SavepointTrigger(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: JobContext) : Action<SavepointOptions, SavepointRequest?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SavepointTrigger::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: SavepointOptions): Result<SavepointRequest?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val savepointRequests = flinkClient.triggerSavepoints(address, listOf(context.getJobId()), params.targetPath)

            return Result(
                ResultStatus.OK,
                savepointRequests.map { SavepointRequest(jobId = it.key, triggerId = it.value) }.firstOrNull()
            )
        } catch (e : Exception) {
            logger.error("Can't trigger savepoint (${context.getJobId()})", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}