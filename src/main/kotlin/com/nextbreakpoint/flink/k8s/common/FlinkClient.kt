package com.nextbreakpoint.flink.k8s.common

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flinkclient.api.FlinkApi
import com.nextbreakpoint.flinkclient.api.JSON
import com.nextbreakpoint.flinkclient.model.AsynchronousOperationResult
import com.nextbreakpoint.flinkclient.model.CheckpointingStatistics
import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion
import com.nextbreakpoint.flinkclient.model.JarFileInfo
import com.nextbreakpoint.flinkclient.model.JarListInfo
import com.nextbreakpoint.flinkclient.model.JarRunResponseBody
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus.StatusEnum
import com.nextbreakpoint.flinkclient.model.JobIdsWithStatusOverview
import com.nextbreakpoint.flinkclient.model.QueueStatus
import com.nextbreakpoint.flinkclient.model.SavepointTriggerRequestBody
import com.nextbreakpoint.flinkclient.model.TaskManagerDetailsInfo
import com.nextbreakpoint.flinkclient.model.TaskManagersInfo
import com.nextbreakpoint.flinkclient.model.TriggerResponse
import com.nextbreakpoint.flink.common.SavepointInfo
import com.nextbreakpoint.flink.common.FlinkAddress
import com.nextbreakpoint.flink.common.Metric
import com.nextbreakpoint.flink.common.RescaleInfo
import com.nextbreakpoint.flink.common.TaskManagerId
import org.apache.log4j.Logger
import java.io.File
import java.util.concurrent.TimeUnit

object FlinkClient {
    private val logger = Logger.getLogger(FlinkClient::class.simpleName)

    private const val TIMEOUT = 10000L

    fun getOverview(address: FlinkAddress): ClusterOverviewWithVersion {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getOverviewCall(null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't get cluster overview")
                }

                body.source().use { source ->
                    return JSON().deserialize(source.readUtf8Line(), ClusterOverviewWithVersion::class.java)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun listJars(address: FlinkAddress): List<JarFileInfo> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.listJarsCall(null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't list JARs")
                }

                body.source().use { source ->
                    return JSON().deserialize<JarListInfo>(source.readUtf8Line(), JarListInfo::class.java).files
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun deleteJars(address: FlinkAddress, files: List<JarFileInfo>) {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            files.forEach {
                val response = flinkApi.deleteJarCall(it.id, null, null).execute()

                response.body().use { body ->
                    if (!response.isSuccessful) {
                        throw CallException("[$address] Can't remove JAR")
                    }
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun listRunningJobs(address: FlinkAddress): List<String> {
        return listJobs(address, setOf(StatusEnum.RUNNING)).keys.toList()
    }

    fun listJobs(address: FlinkAddress, statuses: Set<StatusEnum>): Map<String, StatusEnum> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getJobsCall( null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't get jobs")
                }

                body.source().use { source ->
                    val jobsOverview = JSON().deserialize<JobIdsWithStatusOverview>(source.readUtf8Line(), JobIdsWithStatusOverview::class.java)

                    return jobsOverview.jobs.filter {
                        jobIdWithStatus -> statuses.isEmpty() || statuses.contains(jobIdWithStatus.status)
                    }.map {
                        it.id to it.status
                    }.toMap()
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun runJar(
        address: FlinkAddress,
        jarFileId: String,
        className: String,
        parallelism: Int,
        savepointPath: String?,
        arguments: List<String>
    ): JarRunResponseBody {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT * 10)

            val response = flinkApi.runJarCall(
                jarFileId,
                false,
                if (savepointPath == "") null else savepointPath,
                arguments.joinToString(separator = " "),
                null,
                className,
                parallelism,
                null,
                null
            ).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't run JAR")
                }

                body.source().use { source ->
                    return JSON().deserialize<JarRunResponseBody>(source.readUtf8Line(), JarRunResponseBody::class.java)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getCheckpointingStatistics(address: FlinkAddress, jobs: List<String>): Map<String, CheckpointingStatistics> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return jobs.map { jobId ->
                val response = flinkApi.getJobCheckpointsCall(jobId, null, null).execute()

                jobId to response
            }.map {
                it.second.body().use { body ->
                    if (!it.second.isSuccessful) {
                        throw CallException("[$address] Can't get checkpointing statistics")
                    }

                    it.first to body.source().use { source ->
                        JSON().deserialize<CheckpointingStatistics>(source.readUtf8Line(), CheckpointingStatistics::class.java)
                    }
                }
            }.toMap()
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun cancelJobs(address: FlinkAddress, jobs: List<String>, targetPath: String?): Map<String, String> {
        return createSavepoints(address, jobs, targetPath, true)
            .map {
                it.key to it.value.requestId
            }.onEach {
                logger.info("[$address] Created savepoint request ${it.second} for job ${it.first}")
            }.toMap()
    }

    fun getJobDetails(address: FlinkAddress, jobId: String): JobDetailsInfo {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getJobDetailsCall(jobId, null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't fetch job details")
                }

                body.source().use { source ->
                    return JSON().deserialize(source.readUtf8Line(), JobDetailsInfo::class.java)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getJobMetrics(address: FlinkAddress, jobId: String, metricKey: String): List<Metric> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getJobMetricsCall(jobId, metricKey, null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't fetch job metrics")
                }

                body.source().use { source ->
                    return JSON().deserialize(source.readUtf8Line(), object : TypeToken<List<Metric>>() {}.type)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getJobManagerMetrics(address: FlinkAddress, metricKey: String): List<Metric> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getJobManagerMetricsCall(metricKey, null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't fetch job manager metrics")
                }

                body.source().use { source ->
                    return JSON().deserialize(source.readUtf8Line(), object : TypeToken<List<Metric>>() {}.type)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getTaskManagerMetrics(address: FlinkAddress, taskmanagerId: TaskManagerId, metricKey: String): List<Metric> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getTaskManagerMetricsCall(taskmanagerId.taskmanagerId, metricKey, null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't fetch task manager metrics")
                }

                body.source().use { source ->
                    return JSON().deserialize(source.readUtf8Line(), object : TypeToken<List<Metric>>() {}.type)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getTaskManagerDetails(address: FlinkAddress, taskmanagerId: TaskManagerId): TaskManagerDetailsInfo {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getTaskManagerDetailsCall(taskmanagerId.taskmanagerId, null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't fetch task manager details")
                }

                body.source().use { source ->
                    return JSON().deserialize(source.readUtf8Line(), object : TypeToken<TaskManagerDetailsInfo>() {}.type)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun terminateJobs(address: FlinkAddress, jobs: List<String>) {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            jobs.forEach {
                val detailsResponse = flinkApi.getJobDetailsCall(it, null, null).execute()

                detailsResponse.body().use { body ->
                    if (!detailsResponse.isSuccessful) {
                        throw CallException("[$address] Can't cancel job $it")
                    }

                    body.source().use { source ->
                        val response = flinkApi.terminateJobCall(it, "cancel", null, null).execute()

                        response.body().use { body ->
                            if (!response.isSuccessful) {
                                throw CallException("[$address] Can't cancel job $it")
                            }
                        }
                    }
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getTaskManagersOverview(address: FlinkAddress): TaskManagersInfo {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getTaskManagersOverviewCall(null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't fetch task managers overview")
                }

                body.source().use { source ->
                    return JSON().deserialize(source.readUtf8Line(), object : TypeToken<TaskManagersInfo>() {}.type)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getSavepointRequestsStatus(address: FlinkAddress, requests: Map<String, String>): Map<String, SavepointInfo> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return requests.map { (jobId, requestId) ->
                val response = flinkApi.getJobSavepointStatusCall(jobId, requestId, null, null).execute()

                jobId to response
            }.map {
                it.second.body().use { body ->
                    if (!it.second.isSuccessful) {
                        throw CallException("[$address] Can't get savepoint status for job ${it.first}")
                    }

                    val asynchronousOperationResult = body.source().use { source ->
                        JSON().deserialize<AsynchronousOperationResult>(source.readUtf8Line(), AsynchronousOperationResult::class.java)
                    }

                    if (asynchronousOperationResult.status.id != QueueStatus.IdEnum.COMPLETED) {
                        logger.info("[$address] Savepoint in progress for job ${it.first}")

                        it.first to SavepointInfo("IN_PROGRESS", null)
                    } else {
                        val operation = asynchronousOperationResult.operation as? Map<String, Object>
                        logger.debug("operation: $operation")
                        val location = operation?.get("location") as? String
                        val failureCause = operation?.get("failure-cause") as? Map<String, Object>

                        if (failureCause == null) {
                            logger.info("[$address] Savepoint completed for job ${it.first}")

                            it.first to SavepointInfo("COMPLETED", location)
                        } else {
                            logger.info("[$address] Savepoint failed for job ${it.first} (${failureCause})")

                            it.first to SavepointInfo("FAILED", null)
                        }
                    }
                }
            }.toMap()
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun triggerSavepoints(address: FlinkAddress, jobs: List<String>, targetPath: String?): Map<String, String> {
        return createSavepoints(address, jobs, targetPath, false)
            .map {
                it.key to it.value.requestId
            }.onEach {
                logger.info("[$address] Created savepoint request ${it.second} for job ${it.first}")
            }.toMap()
    }

    private fun createSavepoints(address: FlinkAddress, jobs: List<String>, targetPath: String?, cancelJob: Boolean): Map<String, TriggerResponse> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return jobs.map {
                val requestBody = SavepointTriggerRequestBody().cancelJob(cancelJob).targetDirectory(targetPath)

                val response = flinkApi.createJobSavepointCall(requestBody, it, null, null).execute()

                it to response
            }.map {
                it.second.body().use { body ->
                    if (!it.second.isSuccessful) {
                        throw CallException("[$address] Can't trigger savepoint for job $it")
                    }

                    it.first to body.source().use { source ->
                        JSON().deserialize<TriggerResponse>(source.readUtf8Line(), TriggerResponse::class.java)
                    }
                }
            }.toMap()
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun uploadJar(address: FlinkAddress, file: File): JarUploadResponseBody {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT * 30)

            val response = flinkApi.uploadJarCall(file, null, null).execute();

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't upload JAR")
                }

                body.source().use { source ->
                    return JSON().deserialize(source.readUtf8Line(), object : TypeToken<JarUploadResponseBody>() {}.type)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getRescaleRequestsStatus(address: FlinkAddress, requests: Map<String, String>): Map<String, RescaleInfo> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return requests.map { (jobId, requestId) ->
                val response = flinkApi.getJobRescalingStatusCall(jobId, requestId, null, null).execute()

                jobId to response
            }.map {
                it.second.body().use { body ->
                    if (!it.second.isSuccessful) {
                        throw CallException("[$address] Can't get rescale status for job ${it.first}")
                    }

                    val asynchronousOperationResult = body.source().use { source ->
                        JSON().deserialize<AsynchronousOperationResult>(source.readUtf8Line(), AsynchronousOperationResult::class.java)
                    }

                    if (asynchronousOperationResult.status.id != QueueStatus.IdEnum.COMPLETED) {
                        logger.info("[$address] Rescale in progress for job ${it.first}")

                        it.first to RescaleInfo("IN_PROGRESS")
                    } else {
                        val operation = asynchronousOperationResult.operation as? Map<String, Object>

                        logger.debug("operation: $operation")

                        val failureCause = operation?.get("failure-cause") as? Map<String, Object>

                        if (failureCause == null) {
                            logger.info("[$address] Rescale completed for job ${it.first}")

                            it.first to RescaleInfo("COMPLETED")
                        } else {
                            logger.info("[$address] Rescale failed for job ${it.first} (${failureCause})")

                            it.first to RescaleInfo("FAILED")
                        }
                    }
                }
            }.toMap()
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun triggerJobsRescaling(address: FlinkAddress, jobs: List<String>, parallelism: Int): Map<String, String> {
        return triggerJobRescaling(address, jobs, parallelism)
            .map {
                it.key to it.value.requestId
            }.onEach {
                logger.info("[$address] Created rescale request ${it.second} for job ${it.first}")
            }.toMap()
    }

    private fun triggerJobRescaling(address: FlinkAddress, jobs: List<String>, parallelism: Int): Map<String, TriggerResponse> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return jobs.map {
                val response = flinkApi.triggerJobRescalingCall(it, parallelism, null, null).execute()

                it to response
            }.map {
                it.second.body().use { body ->
                    if (!it.second.isSuccessful) {
                        throw CallException("[$address] Can't trigger rescale for job $it")
                    }

                    it.first to body.source().use { source ->
                        JSON().deserialize<TriggerResponse>(source.readUtf8Line(), TriggerResponse::class.java)
                    }
                }
            }.toMap()
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    private fun createFlinkApiClient(address: FlinkAddress, timeout: Long): FlinkApi {
        val flinkApi = FlinkApi()
        val apiClient = flinkApi.apiClient
        apiClient.basePath = "http://${address.host}:${address.port}"
        apiClient.httpClient.setConnectTimeout(timeout, TimeUnit.MILLISECONDS)
        apiClient.httpClient.setWriteTimeout(timeout, TimeUnit.MILLISECONDS)
        apiClient.httpClient.setReadTimeout(timeout, TimeUnit.MILLISECONDS)
        apiClient.isDebugging = System.getProperty("flink.client.debugging", "false")!!.toBoolean()
        return flinkApi
    }
}