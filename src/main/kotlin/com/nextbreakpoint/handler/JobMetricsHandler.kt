package com.nextbreakpoint.handler

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.flinkclient.api.FlinkApi
import com.nextbreakpoint.model.JobMetrics
import com.nextbreakpoint.model.JobDescriptor
import com.nextbreakpoint.model.Metric
import io.kubernetes.client.JSON
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger
import java.util.*
import java.util.List

object JobMetricsHandler {
    private val logger = Logger.getLogger(JobMetricsHandler::class.simpleName)

    fun execute(portForward: Int?, useNodePort: Boolean, jobDescriptor: JobDescriptor): String {
        val coreApi = CoreV1Api()

        var jobmanagerHost = "localhost"
        var jobmanagerPort = portForward ?: 8081

        if (portForward == null && useNodePort) {
            val nodes = coreApi.listNode(
                false,
                null,
                null,
                null,
                null,
                1,
                null,
                30,
                null
            )

            if (!nodes.items.isEmpty()) {
                nodes.items.get(0).status.addresses.filter {
                    it.type.equals("InternalIP")
                }.map {
                    it.address
                }.firstOrNull()?.let {
                    jobmanagerHost = it
                }
            } else {
                throw RuntimeException("Node not found")
            }
        }

        if (portForward == null) {
            val services = coreApi.listNamespacedService(
                jobDescriptor.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${jobDescriptor.descriptor.name},environment=${jobDescriptor.descriptor.environment},role=jobmanager",
                1,
                null,
                30,
                null
            )

            if (!services.items.isEmpty()) {
                val service = services.items.get(0)

                logger.info("Found JobManager ${service.metadata.name}")

                if (useNodePort) {
                    service.spec.ports.filter {
                        it.name.equals("ui")
                    }.filter {
                        it.nodePort != null
                    }.map {
                        it.nodePort
                    }.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                } else {
                    service.spec.ports.filter {
                        it.name.equals("ui")
                    }.filter {
                        it.port != null
                    }.map {
                        it.port
                    }.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                    jobmanagerHost = service.spec.clusterIP
                }
            } else {
                throw RuntimeException("JobManager not found")
            }
        }

        val flinkApi = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort)

//        val response = flinkApi.getJobMetricsCall(jobDescriptor.jobId, null, null, null).execute()
//        if (response.isSuccessful) {
//            logger.info(response.body().string())
//        }

        try {
            val metrics = getMetric(
                flinkApi,
                jobDescriptor.jobId,
                "totalNumberOfCheckpoints,numberOfCompletedCheckpoints,numberOfInProgressCheckpoints,numberOfFailedCheckpoints,lastCheckpointDuration,lastCheckpointSize,lastCheckpointRestoreTimestamp,lastCheckpointAlignmentBuffered,lastCheckpointExternalPath,fullRestarts,restartingTime,uptime,downtime"
            )

            val metricsMap = metrics.map { metric -> metric.id to metric.value }.toMap()

            val metricsResponse = JobMetrics(
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

            return Gson().toJson(metricsResponse)
        } catch (e : Exception) {
            e.printStackTrace()
        }

        return Gson().toJson(mapOf<String, String>())
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