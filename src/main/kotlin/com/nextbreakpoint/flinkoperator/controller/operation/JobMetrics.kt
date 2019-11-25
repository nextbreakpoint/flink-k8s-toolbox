package com.nextbreakpoint.flinkoperator.controller.operation

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.JobStats
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import io.kubernetes.client.JSON
import org.apache.log4j.Logger

class JobMetrics(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobMetrics::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkClient.listRunningJobs(address)

            if (runningJobs.isEmpty()) {
                logger.info("[name=${clusterId.name}] Can't find a running job")

                return Result(
                    ResultStatus.AWAIT,
                    "{}"
                )
            }

            if (runningJobs.size > 1) {
                logger.warn("[name=${clusterId.name}] There are multiple jobs running")
            }

            val metrics = flinkClient.getJobMetrics(address, runningJobs.first(),
                "totalNumberOfCheckpoints,numberOfCompletedCheckpoints,numberOfInProgressCheckpoints,numberOfFailedCheckpoints,lastCheckpointDuration,lastCheckpointSize,lastCheckpointRestoreTimestamp,lastCheckpointAlignmentBuffered,lastCheckpointExternalPath,fullRestarts,restartingTime,uptime,downtime"
            )

            val metricsMap = metrics.map { metric -> metric.id to metric.value }.toMap()

            val metricsResponse = JobStats(
                totalNumberOfCheckpoints = metricsMap.get("totalNumberOfCheckpoints")?.toInt() ?: 0,
                numberOfCompletedCheckpoints = metricsMap.get("numberOfCompletedCheckpoints")?.toInt() ?: 0,
                numberOfInProgressCheckpoints = metricsMap.get("numberOfInProgressCheckpoints")?.toInt() ?: 0,
                numberOfFailedCheckpoints = metricsMap.get("numberOfFailedCheckpoints")?.toInt() ?: 0,
                lastCheckpointDuration = metricsMap.get("lastCheckpointDuration")?.toLong() ?: 0,
                lastCheckpointSize = metricsMap.get("lastCheckpointSize")?.toLong() ?: 0,
                lastCheckpointRestoreTimestamp = metricsMap.get("lastCheckpointRestoreTimestamp")?.toLong() ?: 0L,
                lastCheckpointAlignmentBuffered = metricsMap.get("lastCheckpointAlignmentBuffered")?.toLong() ?: 0L,
                lastCheckpointExternalPath = metricsMap.get("lastCheckpointExternalPath") ?: "",
                fullRestarts = metricsMap.get("fullRestarts")?.toInt() ?: 0,
                restartingTime = metricsMap.get("restartingTime")?.toLong() ?: 0L,
                uptime = metricsMap.get("uptime")?.toLong() ?: 0L,
                downtime = metricsMap.get("downtime")?.toLong() ?: 0L
            )

            return Result(
                ResultStatus.SUCCESS,
                JSON().serialize(metricsResponse)
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't get metrics of job", e)

            return Result(
                ResultStatus.FAILED,
                "{}"
            )
        }
    }
}