package com.nextbreakpoint.flinkoperator.server.controller.action

import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.FlinkOptions
import com.nextbreakpoint.flinkoperator.server.common.FlinkClient
import com.nextbreakpoint.flinkoperator.server.common.KubeClient
import com.nextbreakpoint.flinkoperator.server.controller.core.Action
import com.nextbreakpoint.flinkoperator.server.controller.core.Result
import com.nextbreakpoint.flinkoperator.server.controller.core.ResultStatus
import io.kubernetes.client.JSON
import org.apache.log4j.Logger

class TaskManagersList(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersList::class.simpleName)
    }

    override fun execute(clusterSelector: ClusterSelector, params: Void?): Result<String> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val overview = flinkClient.getTaskManagersOverview(address)

            Result(
                ResultStatus.OK,
                JSON().serialize(overview.taskmanagers)
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterSelector.name}] Can't get list of task managers", e)

            Result(
                ResultStatus.ERROR,
                "{}"
            )
        }
    }
}