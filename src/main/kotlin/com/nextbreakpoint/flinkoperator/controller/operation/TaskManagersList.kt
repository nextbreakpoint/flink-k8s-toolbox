package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import io.kubernetes.client.JSON
import org.apache.log4j.Logger

class TaskManagersList(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<Void?, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersList::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): OperationResult<String> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val overview = flinkClient.getTaskManagersOverview(address)

            OperationResult(
                OperationStatus.OK,
                JSON().serialize(overview.taskmanagers)
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get list of task managers", e)

            OperationResult(
                OperationStatus.ERROR,
                "{}"
            )
        }
    }
}