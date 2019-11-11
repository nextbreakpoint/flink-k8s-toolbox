package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.ManualAction
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.model.StopOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorAnnotations
import com.nextbreakpoint.flinkoperator.controller.OperatorCache
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class RequestClusterStop(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext, private val cache: OperatorCache) : OperatorCommand<StopOptions, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterStop::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: StopOptions): Result<Void?> {
        try {
            val flinkCluster = cache.getFlinkCluster(clusterId)

            OperatorAnnotations.setWithoutSavepoint(flinkCluster, params.withoutSavepoint)
            OperatorAnnotations.setDeleteResources(flinkCluster, params.deleteResources)
            OperatorAnnotations.setManualAction(flinkCluster, ManualAction.STOP)

            kubernetesContext.updateAnnotations(clusterId, flinkCluster.metadata?.annotations.orEmpty())

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't annotate cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}