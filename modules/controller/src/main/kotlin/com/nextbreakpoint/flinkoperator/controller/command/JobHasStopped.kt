package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.utils.FlinkServerUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class JobHasStopped(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JobHasStopped::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            val flinkApi = FlinkServerUtils.find(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkApi.jobs.jobs.filter {
                    jobIdWithStatus -> jobIdWithStatus.status.value.equals("RUNNING")
            }.map {
                it.id
            }.toList()

            if (runningJobs.isEmpty()) {
                return Result(
                    ResultStatus.SUCCESS,
                    null
                )
            } else {
                logger.info("Can't find a running job in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    null
                )
            }
        } catch (e : Exception) {
            logger.warn("Can't get status of job of cluster ${clusterId.name}")

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}