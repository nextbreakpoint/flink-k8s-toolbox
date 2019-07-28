package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Flink
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import org.apache.log4j.Logger

class JobStop(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Map<String, String>>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(JobStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Map<String, String>> {
        try {
            val flinkApi = Flink.find(flinkOptions, clusterId.namespace, clusterId.name)

            logger.info("Stopping job of cluster ${clusterId.name}...")

            flinkApi.jobs.jobs.forEach {
                val response = flinkApi.terminateJobCall(it.id.toString(), "cancel", null, null).execute()

                if (!it.status.equals("CANCELED") && !response.isSuccessful) {
                    logger.warn("Can't terminate job ${it.id} in cluster ${clusterId.name}")
                }
            }

            val runningJobs = flinkApi.jobs.jobs.filter { it.status.name.equals("RUNNING") }.toList()

            if (runningJobs.isEmpty()) {
                return Result(ResultStatus.SUCCESS, mapOf())
            } else {
                return Result(ResultStatus.AWAIT, mapOf())
            }
        } catch (e : Exception) {
            logger.error("Can't stop jobs of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, mapOf())
        }
    }
}