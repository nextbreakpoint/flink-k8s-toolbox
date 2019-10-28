package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import org.apache.log4j.Logger

class PodsRestart(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<ClusterResources, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(PodsRestart::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): Result<Void?> {
        try {
            logger.info("Restarting pods of cluster ${clusterId.name}...")

            kubernetesContext.restartJobManagerStatefulSets(clusterId, params)

            kubernetesContext.restartTaskManagerStatefulSets(clusterId, params)

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't restart pods of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}