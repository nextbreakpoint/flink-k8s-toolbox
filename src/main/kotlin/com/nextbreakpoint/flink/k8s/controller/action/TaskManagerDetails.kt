package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.JSON
import org.apache.log4j.Logger

class TaskManagerDetails(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient): Action<TaskManagerId, String>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagerDetails::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: TaskManagerId): Result<String> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val details = flinkClient.getTaskManagerDetails(address, params)

            Result(
                ResultStatus.OK,
                JSON().serialize(details)
            )
        } catch (e : Exception) {
            logger.error("Can't get taskmanager's details ($params)", e)

            Result(
                ResultStatus.ERROR,
                "{}"
            )
        }
    }
}