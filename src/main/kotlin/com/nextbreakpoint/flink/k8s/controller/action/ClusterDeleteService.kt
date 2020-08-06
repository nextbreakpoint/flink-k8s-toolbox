package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class ClusterDeleteService(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<Void?, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterDeleteService::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: Void?): Result<Void?> {
        return try {
            kubeClient.deleteService(clusterSelector)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't delete service", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}