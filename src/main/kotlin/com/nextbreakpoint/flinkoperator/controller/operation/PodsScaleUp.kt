package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ClusterScaling
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class PodsScaleUp(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<ClusterScaling, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(PodsScaleUp::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterScaling): OperationResult<Void?> {
        try {
            logger.debug("[name=${clusterId.name}] Restarting pods...")

            kubeClient.restartJobManagerStatefulSets(clusterId, 1)

            kubeClient.restartTaskManagerStatefulSets(clusterId, params.taskManagers)

            return OperationResult(
                OperationStatus.COMPLETED,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't restart pods", e)

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}