package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.SavepointOptions
import com.nextbreakpoint.flinkoperator.common.model.SavepointRequest
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class JobCancel(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<SavepointOptions, SavepointRequest?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(JobCancel::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: SavepointOptions): Result<SavepointRequest?> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkContext.listRunningJobs(address)

            if (runningJobs.size != 1) {
                logger.warn("Expected exactly one job running in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }

            val inprogressCheckpoints = flinkContext.getCheckpointingStatistics(address, runningJobs)

            if (inprogressCheckpoints.filter { it.value.counts.inProgress > 0 }.isNotEmpty()) {
                logger.warn("Savepoint already in progress in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }

            val requests = runningJobs.map {
                logger.info("Cancelling job $it of cluster ${clusterId.name}...")

                val response = flinkContext.createSavepoint(address, it, params.targetPath)

                it to response.requestId
            }.onEach {
                logger.info("Created savepoint request ${it.second} for job ${it.first} of cluster ${clusterId.name}")
            }.toMap()

            return Result(
                ResultStatus.SUCCESS,
                requests.map {
                    SavepointRequest(
                        jobId = it.key,
                        triggerId = it.value
                    )
                }.first()
            )
        } catch (e : Exception) {
            logger.error("Can't trigger savepoint for job of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}