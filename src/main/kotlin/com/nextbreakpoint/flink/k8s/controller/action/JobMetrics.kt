package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.JobStats
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import java.util.logging.Level
import java.util.logging.Logger

class JobMetrics(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : JobAction<String, JobStats?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(JobMetrics::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: String): Result<JobStats?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val metrics = flinkClient.getJobMetrics(address, params,
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
                ResultStatus.OK,
                metricsResponse
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't get job's metrics ($params)", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}