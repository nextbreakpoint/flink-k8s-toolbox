package com.nextbreakpoint.flinkoperator.controller.command

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flinkoperator.common.utils.FlinkServerUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.JobStats
import com.nextbreakpoint.flinkoperator.common.model.Metric
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkclient.api.FlinkApi
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import io.kubernetes.client.JSON
import org.apache.log4j.Logger
import java.util.LinkedList
import java.util.List

class JobMetrics(flinkOptions: FlinkOptions) : OperatorCommand<Void?, String>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JobMetrics::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
        try {
            val flinkApi = FlinkServerUtils.find(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkApi.jobs.jobs.filter {
                    jobIdWithStatus -> jobIdWithStatus.status.value.equals("RUNNING")
            }.map {
                it.id
            }.toList()

            if (runningJobs.size > 1) {
                logger.warn("There are multiple jobs running in cluster ${clusterId.name}")
            }

            if (runningJobs.isNotEmpty()) {
                val metrics = getMetric(
                    flinkApi,
                    runningJobs.first(),
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
                    Gson().toJson(metricsResponse)
                )
            } else {
                logger.info("Can't find a running job in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    "{}"
                )
            }
        } catch (e : Exception) {
            logger.error("Can't get metrics of job of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                "{}"
            )
        }
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