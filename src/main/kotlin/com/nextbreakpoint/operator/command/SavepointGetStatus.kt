package com.nextbreakpoint.operator.command

import com.google.gson.Gson
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.flinkclient.model.AsynchronousOperationResult
import com.nextbreakpoint.flinkclient.model.CheckpointingStatistics
import com.nextbreakpoint.flinkclient.model.QueueStatus
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class SavepointGetStatus(flinkOptions: FlinkOptions) : OperatorCommand<Map<String, String>, String>(flinkOptions) {
    private val logger = Logger.getLogger(SavepointGetStatus::class.simpleName)

    override fun execute(clusterId: ClusterId, params: Map<String, String>): Result<String> {
        try {
            val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

            val inprogressRequests = params.map { (jobId, requestId) ->
                val response = flinkApi.getJobSavepointStatusCall(jobId, requestId, null, null).execute()

                if (response.code() != 200) {
                    logger.error("Can't get savepoint status for job $jobId in cluster ${clusterId.name}")
                }

                jobId to response
            }.filter {
                it.second.code() == 200
            }.map {
                val asynchronousOperationResult = Gson().fromJson(it.second.body().source().readUtf8Line(), AsynchronousOperationResult::class.java)

                if (asynchronousOperationResult.status.id != QueueStatus.IdEnum.COMPLETED) {
                    logger.info("Savepoint still in progress for job ${it.first} in cluster ${clusterId.name}")
                }

                it.first to asynchronousOperationResult.status.id
            }.filter {
                it.second == QueueStatus.IdEnum.IN_PROGRESS
            }.toMap()

            if (inprogressRequests.isEmpty()) {
                val savepoints = params.map { (jobId, _) ->
                    val response = flinkApi.getJobCheckpointsCall(jobId, null, null).execute()

                    if (response.code() != 200) {
                        logger.error("Can't get checkpointing statistics for job $jobId in cluster ${clusterId.name}")
                    }

                    jobId to response
                }.filter {
                    it.second.code() == 200
                }.map {
                    val checkpointingStatistics = Gson().fromJson(it.second.body().source().readUtf8Line(), CheckpointingStatistics::class.java)

                    val savepoint = checkpointingStatistics.latest?.savepoint

                    if (savepoint == null) {
                        logger.error("Savepoint not found for job ${it.first} in cluster ${clusterId.name}")
                    }

                    val externalPathOrEmpty = savepoint?.externalPath ?: ""

                    it.first to externalPathOrEmpty.trim('\"')
                }.filter {
                    it.second.isNotBlank()
                }.toMap()

                if (savepoints.isNotEmpty()) {
                    return Result(ResultStatus.SUCCESS, Gson().toJson(savepoints.values.first()))
                } else {
                    logger.error("Expected at least one savepoint in cluster ${clusterId.name}")

                    return Result(ResultStatus.FAILED, "{}")
                }
            } else {
                return Result(ResultStatus.AWAIT, "{}")
            }
        } catch (e : Exception) {
            logger.error("Can't get savepoint status of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, "{}")
        }
    }
}