package com.nextbreakpoint.flinkoperator.common.utils

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flinkclient.api.FlinkApi
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
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkJobSpec
import com.nextbreakpoint.flinkoperator.common.model.FlinkAddress
import com.nextbreakpoint.flinkoperator.common.model.Metric
import com.nextbreakpoint.flinkoperator.common.model.TaskManagerId
import org.apache.log4j.Logger
import java.io.File
import java.util.concurrent.TimeUnit

object FlinkContext {
    private val logger = Logger.getLogger(FlinkContext::class.simpleName)

    fun getOverview(address: FlinkAddress): ClusterOverviewWithVersion {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.getOverviewCall(null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't get cluster overview - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), ClusterOverviewWithVersion::class.java)
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun listJars(address: FlinkAddress): List<JarFileInfo> {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.listJarsCall(null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't list JARs - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), JarListInfo::class.java).files
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun deleteJars(address: FlinkAddress, files: List<JarFileInfo>) {
        try {
            val flinkApi = createFlinkClient(address)

            files.forEach {
                val response = flinkApi.deleteJarCall(it.id, null, null).execute()

                if (!response.isSuccessful) {
                    throw CallException("Can't remove JAR - $address")
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
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.getJobsCall( null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't get jobs - $address")
            }

            response.body().use {
                val jobsOverview = Gson().fromJson(it.source().readUtf8Line(), JobIdsWithStatusOverview::class.java)

                return jobsOverview.jobs.filter {
                    jobIdWithStatus -> jobIdWithStatus.status == JobIdWithStatus.StatusEnum.RUNNING
                }.map {
                    it.id
                }.toList()
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun listJobs(address: FlinkAddress): List<JobIdWithStatus> {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.getJobsCall( null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't get jobs - $address")
            }

            response.body().use {
                val jobsOverview = Gson().fromJson(it.source().readUtf8Line(), JobIdsWithStatusOverview::class.java)

                return jobsOverview.jobs
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun runJar(address: FlinkAddress, jarFile: JarFileInfo, flinkJob: V1FlinkJobSpec, savepointPath: String?) {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.runJarCall(
                jarFile.id,
                false,
                savepointPath,
                flinkJob.arguments.joinToString(separator = " "),
                null,
                flinkJob.className,
                flinkJob.parallelism,
                null,
                null
            ).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't run JAR - $address")
            }

            response.body().use {
                logger.debug("Job started: ${it.source().readUtf8Line()}")
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getCheckpointingStatistics(address: FlinkAddress, jobs: List<String>): Map<String, CheckpointingStatistics> {
        try {
            val flinkApi = createFlinkClient(address)

            return jobs.map { jobId ->
                val response = flinkApi.getJobCheckpointsCall(jobId, null, null).execute()

                if (!response.isSuccessful) {
                    throw CallException("Can't get checkpointing statistics - $address")
                }

                jobId to response
            }.filter {
                it.second.code() == 200
            }.map {
                it.first to it.second.body().use {
                    Gson().fromJson(it.source().readUtf8Line(), CheckpointingStatistics::class.java)
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
            val flinkApi = createFlinkClient(address)

            val requestBody = SavepointTriggerRequestBody().cancelJob(true).targetDirectory(targetPath)

            val response = flinkApi.createJobSavepointCall(requestBody, it, null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't request savepoint - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), TriggerResponse::class.java)
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getJobDetails(address: FlinkAddress, jobId: String): JobDetailsInfo {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.getJobDetailsCall(jobId, null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't fetch job details - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), JobDetailsInfo::class.java)
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getJobMetrics(address: FlinkAddress, jobId: String, metricKey: String): List<Metric> {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.getJobMetricsCall(jobId, metricKey, null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't fetch job metrics - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), object : TypeToken<List<Metric>>() {}.type)
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getJobManagerMetrics(address: FlinkAddress, metricKey: String): List<Metric> {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.getJobManagerMetricsCall(metricKey, null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't fetch job manager metrics - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), object : TypeToken<List<Metric>>() {}.type)
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getTaskManagerMetrics(address: FlinkAddress, taskmanagerId: TaskManagerId, metricKey: String): List<Metric> {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.getTaskManagerMetricsCall(taskmanagerId.taskmanagerId, metricKey, null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't fetch task manager metrics - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), object : TypeToken<List<Metric>>() {}.type)
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getTaskManagerDetails(address: FlinkAddress, taskmanagerId: TaskManagerId): TaskManagerDetailsInfo {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.getTaskManagerDetailsCall(taskmanagerId.taskmanagerId, null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't fetch task manager details - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), object : TypeToken<TaskManagerDetailsInfo>() {}.type)
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun terminateJobs(address: FlinkAddress, jobs: List<String>) {
        try {
            val flinkApi = createFlinkClient(address)

            jobs.forEach {
                val response = flinkApi.terminateJobCall(it, "cancel", null, null).execute()

                if (!response.isSuccessful) {
                    logger.warn("Can't cancel job $it - $address");
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
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.getTaskManagersOverviewCall(null, null).execute()

            if (!response.isSuccessful) {
                throw CallException("Can't fetch task managers overview - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), object : TypeToken<TaskManagersInfo>() {}.type)
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getPendingSavepointRequests(address: FlinkAddress, requests: Map<String, String>): List<String> {
        try {
            val flinkApi = createFlinkClient(address)

            return requests.map { (jobId, requestId) ->
                val response = flinkApi.getJobSavepointStatusCall(jobId, requestId, null, null).execute()

                if (!response.isSuccessful) {
                    logger.error("Can't get savepoint status for job $jobId - $address")
                }

                jobId to response
            }.filter {
                it.second.isSuccessful
            }.map {
                val asynchronousOperationResult = it.second.body().use {
                    Gson().fromJson(it.source().readUtf8Line(), AsynchronousOperationResult::class.java)
                }

                if (asynchronousOperationResult.status.id != QueueStatus.IdEnum.COMPLETED) {
                    logger.info("Savepoint still in progress for job ${it.first} - $address")
                }

                it.first to asynchronousOperationResult.status.id
            }.filter {
                it.second == QueueStatus.IdEnum.IN_PROGRESS
            }.map {
                it.first
            }.toList()
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun getLatestSavepointPaths(address: FlinkAddress, requests: Map<String, String>): Map<String, String> {
        try {
            val flinkApi = createFlinkClient(address)

            return requests.map { (jobId, _) ->
                val response = flinkApi.getJobCheckpointsCall(jobId, null, null).execute()

                if (!response.isSuccessful) {
                    logger.error("Can't get checkpointing statistics for job $jobId - $address")
                }

                jobId to response
            }.filter {
                it.second.isSuccessful
            }.map {
                val checkpointingStatistics = it.second.body().use {
                    Gson().fromJson(it.source().readUtf8Line(), CheckpointingStatistics::class.java)
                }

                val savepoint = checkpointingStatistics.latest?.savepoint

                if (savepoint == null) {
                    logger.error("Savepoint not found for job ${it.first} - $address")
                }

                val externalPathOrEmpty = savepoint?.externalPath ?: ""

                it.first to externalPathOrEmpty.trim('\"')
            }.filter {
                it.second.isNotBlank()
            }.toMap()
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun triggerSavepoints(address: FlinkAddress, jobs: List<String>, targetPath: String?): Map<String, String>  {
        try {
            val flinkApi = createFlinkClient(address)

            return jobs.map {
                val requestBody = SavepointTriggerRequestBody().cancelJob(false).targetDirectory(targetPath)

                val response = flinkApi.createJobSavepointCall(requestBody, it, null, null).execute()

                if (!response.isSuccessful) {
                    logger.warn("Can't request savepoint for job $it - $address")
                }

                it to response
            }.filter {
                it.second.isSuccessful
            }.map {
                it.first to it.second.body().use {
                    Gson().fromJson(it.source().readUtf8Line(), TriggerResponse::class.java)
                }
            }.map {
                it.first to it.second.requestId
            }.onEach {
                logger.info("Created savepoint request ${it.second} for job ${it.first} - $address")
            }.toMap()
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    fun uploadJarCall(address: FlinkAddress, file: File): JarUploadResponseBody {
        try {
            val flinkApi = createFlinkClient(address)

            val response = flinkApi.uploadJarCall(file, null, null).execute();

            if (!response.isSuccessful) {
                throw CallException("Can't upload JAR - $address")
            }

            response.body().use {
                return Gson().fromJson(it.source().readUtf8Line(), object : TypeToken<JarUploadResponseBody>() {}.type)
            }
        } catch (e : CallException) {
            throw e
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    private fun createFlinkClient(address: FlinkAddress): FlinkApi {
        val flinkApi = FlinkApi()
        flinkApi.apiClient.basePath = "http://${address.host}:${address.port}"
        flinkApi.apiClient.httpClient.setConnectTimeout(20000, TimeUnit.MILLISECONDS)
        flinkApi.apiClient.httpClient.setWriteTimeout(30000, TimeUnit.MILLISECONDS)
        flinkApi.apiClient.httpClient.setReadTimeout(30000, TimeUnit.MILLISECONDS)
//        flinkApi.apiClient.isDebugging = true
        return flinkApi
    }
}