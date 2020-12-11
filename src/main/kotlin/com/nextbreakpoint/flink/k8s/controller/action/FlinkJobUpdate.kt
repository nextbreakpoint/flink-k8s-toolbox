package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import java.util.logging.Level
import java.util.logging.Logger

class FlinkJobUpdate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : JobAction<V1FlinkJob, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(FlinkJobUpdate::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: V1FlinkJob): Result<Void?> {
        return try {
            kubeClient.updateFlinkJob(namespace, params)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't update job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}