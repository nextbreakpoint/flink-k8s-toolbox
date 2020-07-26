package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
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

    override fun execute(clusterSelector: ClusterSelector, params: ClusterScaling): OperationResult<Void?> {
        return try {
            logger.debug("[name=${clusterSelector.name}] Restarting pods...")

            kubeClient.restartJobManagerStatefulSets(clusterSelector, 1)

            kubeClient.restartTaskManagerStatefulSets(clusterSelector, params.taskManagers)

            OperationResult(
                OperationStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't restart pods", e)

            OperationResult(
                OperationStatus.ERROR,
                null
            )
        }
    }
}