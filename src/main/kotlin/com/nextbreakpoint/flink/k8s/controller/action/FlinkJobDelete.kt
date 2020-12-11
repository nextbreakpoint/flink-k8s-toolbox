package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import java.util.logging.Level
import java.util.logging.Logger

class FlinkJobDelete(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : JobAction<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(FlinkJobDelete::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: Void?): Result<Void?> {
        return try {
            kubeClient.deleteFlinkJob(namespace, "$clusterName-$jobName")

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't delete job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}