package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.models.V1Job
import java.util.logging.Level
import java.util.logging.Logger

class BatchJobCreate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : JobAction<V1Job, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(BatchJobCreate::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: V1Job): Result<String?> {
        return try {
            val jobOut = kubeClient.createJob(namespace, params)

            Result(
                ResultStatus.OK,
                jobOut.metadata?.name ?: throw RuntimeException("Unexpected metadata")
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't create batch job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}