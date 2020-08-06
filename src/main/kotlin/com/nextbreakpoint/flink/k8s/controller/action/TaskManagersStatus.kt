package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flinkclient.model.TaskManagersInfo
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class TaskManagersStatus(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, TaskManagersInfo?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersStatus::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: Void?): Result<TaskManagersInfo?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, clusterSelector.namespace, clusterSelector.name)

            val overview = flinkClient.getTaskManagersOverview(address)

            return Result(
                ResultStatus.OK,
                overview
            )
        } catch (e : Exception) {
            logger.error("Can't get taskmanagers overview", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}