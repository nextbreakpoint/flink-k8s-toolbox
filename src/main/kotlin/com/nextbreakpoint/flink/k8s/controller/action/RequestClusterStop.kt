package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.k8s.controller.core.ClusterContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class RequestClusterStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: ClusterContext) : ClusterAction<StopOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestClusterStop::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: StopOptions): Result<Void?> {
        return try {
            context.setClusterWithoutSavepoint(params.withoutSavepoint)
            context.setClusterManualAction(Action.STOP)

            kubeClient.updateClusterAnnotations(namespace, clusterName, context.getClusterAnnotations())

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't stop cluster", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}