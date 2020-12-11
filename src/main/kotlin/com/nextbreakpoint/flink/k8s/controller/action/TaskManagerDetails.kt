package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.TaskManagerId
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flinkclient.model.TaskManagerDetailsInfo
import java.util.logging.Level
import java.util.logging.Logger

class TaskManagerDetails(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient): ClusterAction<TaskManagerId, TaskManagerDetailsInfo?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagerDetails::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: TaskManagerId): Result<TaskManagerDetailsInfo?> {
        return try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val details = flinkClient.getTaskManagerDetails(address, params)

            Result(
                ResultStatus.OK,
                details
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't get taskmanager's details ($params)", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}