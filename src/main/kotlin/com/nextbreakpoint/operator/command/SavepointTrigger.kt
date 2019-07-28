package com.nextbreakpoint.operator.command

import com.google.gson.Gson
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.flinkclient.model.CheckpointingStatistics
import com.nextbreakpoint.flinkclient.model.SavepointTriggerRequestBody
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class SavepointTrigger(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Map<String, String>>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(SavepointTrigger::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Map<String, String>> {
        try {
            val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkApi.jobs.jobs.filter {
                jobIdWithStatus -> jobIdWithStatus.status.value.equals("RUNNING")
            }.map {
                it.id
            }.toList()

            if (runningJobs.isEmpty()) {
                logger.info("No running job found in cluster ${clusterId.name}")
            }

            val inprogressCheckpoints = runningJobs.map { jobId ->
                val response = flinkApi.getJobCheckpointsCall(jobId, null, null).execute()

                if (response.code() != 200) {
                    logger.error("Can't get checkpointing statistics for job $jobId in cluster ${clusterId.name}")
                }

                jobId to response
            }.filter {
                it.second.code() == 200
            }.map {
                it.first to it.second.body().use {
                    Gson().fromJson(it.source().readUtf8Line(), CheckpointingStatistics::class.java)
                }
            }.filter {
                it.second.counts.inProgress > 0
            }.toMap()

            if (inprogressCheckpoints.isEmpty()) {
                if (runningJobs.size == 1) {
                    val requests = runningJobs.map {
                        val requestBody = SavepointTriggerRequestBody().cancelJob(false).targetDirectory("file:///var/tmp") //TODO pass in target directory or configure default

                        val response = flinkApi.createJobSavepoint(requestBody, it)

                        it to response.requestId
                    }.onEach {
                        logger.info("Created savepoint request ${it.second} for job ${it.first} in cluster ${clusterId.name}")
                    }.toMap()

                    return Result(ResultStatus.SUCCESS, requests)
                } else {
                    logger.warn("Expected one running job in cluster ${clusterId.name}")

                    return Result(ResultStatus.FAILED, mapOf())
                }
            } else {
                logger.error("Savepoint in progress for cluster ${clusterId.name}")

                return Result(ResultStatus.AWAIT, mapOf())
            }
        } catch (e : Exception) {
            logger.error("Can't trigger savepoint for job of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, mapOf())
        }
    }
}