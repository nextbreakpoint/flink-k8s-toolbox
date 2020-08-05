package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.TaskManagerId
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import io.kubernetes.client.JSON
import org.apache.log4j.Logger

class TaskManagerDetails(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient): Action<TaskManagerId, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagerDetails::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: TaskManagerId): Result<String> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val details = flinkClient.getTaskManagerDetails(address, params)

            Result(
                ResultStatus.OK,
                JSON().serialize(details)
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get details of task manager $params", e)

            Result(
                ResultStatus.ERROR,
                "{}"
            )
        }
    }
}