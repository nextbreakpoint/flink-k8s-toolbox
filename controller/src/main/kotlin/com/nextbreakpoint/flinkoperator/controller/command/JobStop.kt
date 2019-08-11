package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.utils.FlinkServerUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class JobStop(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Map<String, String>>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JobStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Map<String, String>> {
        try {
            val flinkApi = FlinkServerUtils.find(flinkOptions, clusterId.namespace, clusterId.name)

            flinkApi.jobs.jobs.filter { it.status.name.equals("RUNNING") }.forEach {
                logger.info("Stopping job of cluster ${clusterId.name}...")

                val response = flinkApi.terminateJobCall(it.id.toString(), "cancel", null, null).execute()

                if (!response.isSuccessful) {
                    logger.warn("Can't terminate job ${it.id} in cluster ${clusterId.name}")
                }
            }

            val runningJobs = flinkApi.jobs.jobs.filter { it.status.name.equals("RUNNING") }.toList()

            if (runningJobs.isEmpty()) {
                logger.info("Job of cluster ${clusterId.name} has been stopped")

                return Result(
                    ResultStatus.SUCCESS,
                    mapOf()
                )
            } else {
                logger.info("No running job found in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    mapOf()
                )
            }
        } catch (e : Exception) {
            logger.error("Can't stop jobs of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                mapOf()
            )
        }
    }
}