package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class RequestClusterStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: ClusterContext) : Action<StartOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterStart::class.simpleName)
    }

    override fun execute(clusterSelector: ResourceSelector, params: StartOptions): Result<Void?> {
        return try {
            context.setClusterWithoutSavepoint(params.withoutSavepoint)
            context.setClusterManualAction(ManualAction.START)

            kubeClient.updateClusterAnnotations(clusterSelector, context.getClusterAnnotations())

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't start cluster", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}