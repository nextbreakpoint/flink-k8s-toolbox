package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.StopOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class RequestJobStop(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: JobContext) : JobAction<StopOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestJobStop::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: StopOptions): Result<Void?> {
        return try {
            context.setJobWithoutSavepoint(params.withoutSavepoint)
            context.setJobManualAction(Action.STOP)

            kubeClient.updateJobAnnotations(namespace, "$clusterName-$jobName", context.getJobAnnotations())

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't stop job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}