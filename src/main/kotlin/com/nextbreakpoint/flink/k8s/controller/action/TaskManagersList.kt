package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.JSON
import org.apache.log4j.Logger

class TaskManagersList(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersList::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: Void?): Result<String> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val overview = flinkClient.getTaskManagersOverview(address)

            Result(
                ResultStatus.OK,
                JSON().serialize(overview.taskmanagers)
            )
        } catch (e : Exception) {
            logger.error("Can't get list of task managers", e)

            Result(
                ResultStatus.ERROR,
                "{}"
            )
        }
    }
}