package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import org.apache.log4j.Logger

class TaskManagersGetReplicas(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, Int>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersGetReplicas::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): OperationResult<Int> {
        try {
            val replicas = kubeClient.getTaskManagerStatefulSetReplicas(clusterId)

            return OperationResult(
                OperationStatus.COMPLETED,
                replicas
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't get replicas of task managers", e)

            return OperationResult(
                OperationStatus.FAILED,
                0
            )
        }
    }
}