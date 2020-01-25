package com.nextbreakpoint.flinkoperator.common.utils

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flinkclient.api.FlinkApi
import com.nextbreakpoint.flinkclient.api.JSON
import com.nextbreakpoint.flinkclient.model.AsynchronousOperationResult
import com.nextbreakpoint.flinkclient.model.CheckpointingStatistics
import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion
import com.nextbreakpoint.flinkclient.model.JarFileInfo
import com.nextbreakpoint.flinkclient.model.JarListInfo
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus
import com.nextbreakpoint.flinkclient.model.JobIdsWithStatusOverview
import com.nextbreakpoint.flinkclient.model.QueueStatus
import com.nextbreakpoint.flinkclient.model.SavepointTriggerRequestBody
import com.nextbreakpoint.flinkclient.model.TaskManagerDetailsInfo
import com.nextbreakpoint.flinkclient.model.TaskManagersInfo
import com.nextbreakpoint.flinkclient.model.TriggerResponse
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.Metric
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
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
                        jobIdWithStatus -> jobIdWithStatus.status == JobIdWithStatus.StatusEnum.RUNNING
                    }.map {
                        it.id
                    }.toList()
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun listJobs(address: FlinkAddress): List<JobIdWithStatus> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val response = flinkApi.getJobsCall( null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't get jobs")
                }

                body.source().use { source ->
                    val jobsOverview = JSON().deserialize<JobIdsWithStatusOverview>(source.readUtf8Line(), JobIdsWithStatusOverview::class.java)

                    return jobsOverview.jobs
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
        jarFile: JarFileInfo,
        className: String,
        parallelism: Int,
        savepointPath: String?,
        arguments: List<String>
    ) {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT * 10)

            val response = flinkApi.runJarCall(
                jarFile.id,
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
                    logger.debug("[$address] Job started: ${source.readUtf8Line()}")
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

    fun createSavepoint(address: FlinkAddress, it: String, targetPath: String?): TriggerResponse {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            val requestBody = SavepointTriggerRequestBody().cancelJob(true).targetDirectory(targetPath)

            val response = flinkApi.createJobSavepointCall(requestBody, it, null, null).execute()

            response.body().use { body ->
                if (!response.isSuccessful) {
                    throw CallException("[$address] Can't request savepoint")
                }

                body.source().use { source ->
                    return JSON().deserialize(source.readUtf8Line(), TriggerResponse::class.java)
                }
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
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
                val response = flinkApi.terminateJobCall(it, "cancel", null, null).execute()

                response.body().use { body ->
                    if (!response.isSuccessful) {
                        logger.warn("[$address] Can't cancel job $it");
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

    fun getPendingSavepointRequests(address: FlinkAddress, requests: Map<String, String>): List<String> {
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
                        logger.info("[$address] Savepoint still in progress for job ${it.first}")
                    }

                    it.first to asynchronousOperationResult.status.id
                }
            }.filter {
                it.second == QueueStatus.IdEnum.IN_PROGRESS
            }.map {
                it.first
            }.toList()
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getLatestSavepointPaths(address: FlinkAddress, requests: Map<String, String>): Map<String, String> {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return requests.map { (jobId, _) ->
                val response = flinkApi.getJobCheckpointsCall(jobId, null, null).execute()

                jobId to response
            }.map {
                it.second.body().use { body ->
                    if (!it.second.isSuccessful) {
                        throw CallException("[$address] Can't get checkpointing statistics for job ${it.first}")
                    }

                    val checkpointingStatistics = body.source().use { source ->
                        JSON().deserialize<CheckpointingStatistics>(source.readUtf8Line(), CheckpointingStatistics::class.java)
                    }

                    val savepoint = checkpointingStatistics.latest?.savepoint

                    if (savepoint == null) {
                        logger.error("[$address] Savepoint not found for job ${it.first}")
                    }

                    val externalPathOrEmpty = savepoint?.externalPath ?: ""

                    it.first to externalPathOrEmpty.trim('\"')
                }
            }.filter {
                it.second.isNotBlank()
            }.toMap()
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun triggerSavepoints(address: FlinkAddress, jobs: List<String>, targetPath: String?): Map<String, String>  {
        try {
            val flinkApi = createFlinkApiClient(address, TIMEOUT)

            return jobs.map {
                val requestBody = SavepointTriggerRequestBody().cancelJob(false).targetDirectory(targetPath)

                val response = flinkApi.createJobSavepointCall(requestBody, it, null, null).execute()

                it to response
            }.map {
                it.second.body().use { body ->
                    if (!it.second.isSuccessful) {
                        throw CallException("[$address] Can't request savepoint for job $it")
                    }

                    it.first to body.source().use { source ->
                        JSON().deserialize<TriggerResponse>(source.readUtf8Line(), TriggerResponse::class.java)
                    }
                }
            }.map {
                it.first to it.second.requestId
            }.onEach {
                logger.info("[$address] Created savepoint request ${it.second} for job ${it.first}")
            }.toMap()
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun uploadJarCall(address: FlinkAddress, file: File): JarUploadResponseBody {
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

    fun triggerJobRescaling(address: FlinkAddress, parallelism: Int) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
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