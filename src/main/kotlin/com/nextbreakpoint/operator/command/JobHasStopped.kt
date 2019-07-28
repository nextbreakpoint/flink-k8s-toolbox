package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class JobHasStopped(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JobHasStopped::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkApi.jobs.jobs.filter {
                    jobIdWithStatus -> jobIdWithStatus.status.value.equals("RUNNING")
            }.map {
                it.id
            }.toList()

            if (runningJobs.isEmpty()) {
                return Result(ResultStatus.SUCCESS, null)
            } else {
                logger.info("No running job found in cluster ${clusterId.name}")

                return Result(ResultStatus.AWAIT, null)
            }
        } catch (e : Exception) {
            logger.error("Can't get job of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }
}