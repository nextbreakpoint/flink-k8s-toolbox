package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo
import org.apache.log4j.Logger

class JobDetails(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : JobAction<String, JobDetailsInfo?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobDetails::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: String): Result<JobDetailsInfo?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val details = flinkClient.getJobDetails(address, params)

            return Result(
                ResultStatus.OK,
                details
            )
        } catch (e : Exception) {
            logger.error("Can't get job's details ($params)", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}