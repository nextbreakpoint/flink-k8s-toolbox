package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import io.kubernetes.client.JSON
import org.apache.log4j.Logger

class TaskManagersList(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersList::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: Void?): OperationResult<String> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterId.namespace, clusterId.name)

            val overview = flinkClient.getTaskManagersOverview(address)

            return OperationResult(
                OperationStatus.COMPLETED,
                JSON().serialize(overview.taskmanagers)
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't get list of task managers", e)

            return OperationResult(
                OperationStatus.FAILED,
                "{}"
            )
        }
    }
}