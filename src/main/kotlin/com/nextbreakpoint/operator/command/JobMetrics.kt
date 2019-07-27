package com.nextbreakpoint.operator.command

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Metric
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.flinkclient.api.FlinkApi
import com.nextbreakpoint.operator.OperatorCommand
import io.kubernetes.client.JSON
import java.util.LinkedList
import java.util.List

class JobMetrics(flinkOptions: FlinkOptions) : OperatorCommand<Void?, String>(flinkOptions) {
    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
        val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

//        val response = flinkApi.getJobMetricsCall(jobDescriptor.jobId, null, null, null).execute()
//        if (response.isSuccessful) {
//            logger.info(response.body().string())
//        }

//        try {
//            val metrics = getMetric(
//                flinkApi,
//                jobId,
//                "totalNumberOfCheckpoints,numberOfCompletedCheckpoints,numberOfInProgressCheckpoints,numberOfFailedCheckpoints,lastCheckpointDuration,lastCheckpointSize,lastCheckpointRestoreTimestamp,lastCheckpointAlignmentBuffered,lastCheckpointExternalPath,fullRestarts,restartingTime,uptime,downtime"
//            )
//
//            val metricsMap = metrics.map { metric -> metric.id to metric.value }.toMap()
//
//            val metricsResponse = JobStats(
//                totalNumberOfCheckpoints = metricsMap.get("totalNumberOfCheckpoints")?.toInt() ?: 0,
//                numberOfCompletedCheckpoints = metricsMap.get("numberOfCompletedCheckpoints")?.toInt() ?: 0,
//                numberOfInProgressCheckpoints = metricsMap.get("numberOfInProgressCheckpoints")?.toInt() ?: 0,
//                numberOfFailedCheckpoints = metricsMap.get("numberOfFailedCheckpoints")?.toInt() ?: 0,
//                lastCheckpointDuration = metricsMap.get("lastCheckpointDuration")?.toLong() ?: 0,
//                lastCheckpointSize = metricsMap.get("lastCheckpointSize")?.toLong() ?: 0,
//                lastCheckpointRestoreTimestamp = metricsMap.get("lastCheckpointRestoreTimestamp")?.toLong() ?: 0L,
//                lastCheckpointAlignmentBuffered = metricsMap.get("lastCheckpointAlignmentBuffered")?.toLong() ?: 0L,
//                lastCheckpointExternalPath = metricsMap.get("lastCheckpointExternalPath") ?: "",
//                fullRestarts = metricsMap.get("fullRestarts")?.toInt() ?: 0,
//                restartingTime = metricsMap.get("restartingTime")?.toLong() ?: 0L,
//                uptime = metricsMap.get("uptime")?.toLong() ?: 0L,
//                downtime = metricsMap.get("downtime")?.toLong() ?: 0L
//            )
//
//            return Gson().toJson(metricsResponse)
//        } catch (e : Exception) {
//            e.printStackTrace()
//        }

        return Result(ResultStatus.SUCCESS, "{}")
    }

    private fun getMetric(flinkApi: FlinkApi, jobId: String, metricKey: String): List<Metric> {
        val response = flinkApi.getJobMetricsCall(jobId, metricKey, null, null).execute()
        return if (response.isSuccessful) {
//            logger.info(response.body().string())
            response.body().use {
                JSON().deserialize(it.source().readUtf8Line(), object : TypeToken<List<Metric>>() {}.type) as List<Metric>
            }
        } else {
            LinkedList<Metric>() as List<Metric>
        }
    }
}