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

class SavepointTrigger(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<SavepointOptions, SavepointRequest?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(SavepointTrigger::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: SavepointOptions): Result<SavepointRequest?> {
        try {
            val address = kubernetesContext.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val runningJobs = flinkContext.listRunningJobs(address)

            if (runningJobs.size > 1) {
                logger.warn("There are multiple jobs running in cluster ${clusterId.name}")
            }

            if (runningJobs.size != 1) {
                logger.warn("Can't find a running job in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.FAILED,
                    null
                )
            }

            val checkpointingStatistics = flinkContext.getCheckpointingStatistics(address, runningJobs)

            if (checkpointingStatistics.filter { it.value.counts.inProgress > 0 }.isNotEmpty()) {
                logger.warn("Savepoint in progress for job in cluster ${clusterId.name}")

                return Result(
                    ResultStatus.AWAIT,
                    null
                )
            }

            val savepointRequests = flinkContext.triggerSavepoints(address, runningJobs, params.targetPath)

            return Result(
                ResultStatus.SUCCESS,
                savepointRequests.map {
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