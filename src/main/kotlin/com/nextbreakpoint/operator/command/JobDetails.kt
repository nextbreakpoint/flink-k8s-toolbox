package com.nextbreakpoint.operator.command

import com.google.gson.Gson
import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class JobDetails(flinkOptions: FlinkOptions) : OperatorCommand<Void?, String>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JobDetails::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<String> {
        try {
            val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkApi.jobs.jobs.filter {
                    jobIdWithStatus -> jobIdWithStatus.status.value.equals("RUNNING")
            }.map {
                it.id
            }.toList()

            if (runningJobs.size > 1) {
                logger.warn("Multiple jobs running in cluster ${clusterId.name}")
            }

            if (runningJobs.isNotEmpty()) {
                val details = flinkApi.getJobDetails(runningJobs.first())

                return Result(ResultStatus.SUCCESS, Gson().toJson(details))
            } else {
                logger.info("No running job found in cluster ${clusterId.name}")

                return Result(ResultStatus.FAILED, "{}")
            }
        } catch (e : Exception) {
            logger.error("Can't get details of job of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, "{}")
        }
    }
}