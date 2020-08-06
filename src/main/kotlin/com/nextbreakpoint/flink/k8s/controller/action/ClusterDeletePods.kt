package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.DeleteOptions
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class ClusterDeletePods(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Action<DeleteOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterDeletePods::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: DeleteOptions): Result<Void?> {
        return try {
            kubeClient.deletePods(clusterSelector, params)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't delete pods", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}