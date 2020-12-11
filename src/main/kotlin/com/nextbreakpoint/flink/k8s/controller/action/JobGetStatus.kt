package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import java.util.logging.Level
import java.util.logging.Logger

class JobGetStatus(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : JobAction<String, String?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobGetStatus::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: String): Result<String?> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val jobDetails = flinkClient.getJobDetails(address, params)

            Result(
                ResultStatus.OK,
                jobDetails.state.value.toString().toUpperCase()
            )
        } catch (e: Exception) {
            logger.log(Level.WARNING, "Can't get job's status ($params)")

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}
