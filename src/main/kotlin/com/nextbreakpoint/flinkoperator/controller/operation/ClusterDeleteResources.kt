package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import org.apache.log4j.Logger

class ClusterDeleteResources(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<Void?, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterDeleteResources::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            logger.info("Deleting resources of cluster ${clusterId.name}...")

            kubernetesContext.deleteBootstrapJobs(clusterId)

            kubernetesContext.deleteStatefulSets(clusterId)

            kubernetesContext.deleteServices(clusterId)

            kubernetesContext.deletePersistentVolumeClaims(clusterId)

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't delete resources of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}