package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkclient.model.JobIdWithStatus
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import org.apache.log4j.Logger

class JobIsCancelled(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, Boolean>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobIsCancelled::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): Result<Boolean> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val allJobs = flinkClient.listJobs(address, setOf())

            val cancelledJobs = allJobs
                .filter { it.value == JobIdWithStatus.StatusEnum.CANCELED }
                .map { it.key }
                .toList()

            val finishedJobs = allJobs
                .filter { it.value == JobIdWithStatus.StatusEnum.FINISHED }
                .map { it.key }
                .toList()

            val failedJobs = allJobs
                .filter { it.value == JobIdWithStatus.StatusEnum.FAILED }
                .map { it.key }
                .toList()

            if (cancelledJobs.isNotEmpty() && cancelledJobs.size + finishedJobs.size + failedJobs.size == allJobs.size) {
                Result(
                    ResultStatus.OK,
                    true
                )
            } else {
                Result(
                    ResultStatus.OK,
                    false
                )
            }
        } catch (e : Exception) {
            logger.warn("[name=${clusterSelector.name}] Can't list jobs")

            Result(
                ResultStatus.ERROR,
                false
            )
        }
    }
}