package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.SavepointOptions
import com.nextbreakpoint.flink.common.SavepointRequest
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import java.util.logging.Level
import java.util.logging.Logger

class SavepointTrigger(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: JobContext) : JobAction<SavepointOptions, SavepointRequest?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(SavepointTrigger::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: SavepointOptions): Result<SavepointRequest?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val savepointRequests = flinkClient.triggerSavepoints(address, listOf(context.getJobId()), params.targetPath)

            return Result(
                ResultStatus.OK,
                savepointRequests.map { SavepointRequest(jobId = it.key, triggerId = it.value) }.firstOrNull()
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't trigger savepoint (${context.getJobId()})", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}