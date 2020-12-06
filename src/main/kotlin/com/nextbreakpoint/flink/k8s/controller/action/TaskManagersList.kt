package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flinkclient.model.TaskManagerInfo
import org.apache.log4j.Logger

class TaskManagersList(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<Void?, List<TaskManagerInfo>>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersList::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: Void?): Result<List<TaskManagerInfo>> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val overview = flinkClient.getTaskManagersOverview(address)

            Result(
                ResultStatus.OK,
                overview.taskmanagers
            )
        } catch (e : Exception) {
            logger.error("Can't get list of taskmanagers", e)

            Result(
                ResultStatus.ERROR,
                listOf()
            )
        }
    }
}