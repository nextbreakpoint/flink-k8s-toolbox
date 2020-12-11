package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobAction
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import java.util.logging.Level
import java.util.logging.Logger

class RequestJobStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: JobContext) : JobAction<StartOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestJobStart::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, jobName: String, params: StartOptions): Result<Void?> {
        return try {
            context.setJobWithoutSavepoint(params.withoutSavepoint)
            context.setJobManualAction(Action.START)

            kubeClient.updateJobAnnotations(namespace, "$clusterName-$jobName", context.getJobAnnotations())

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't start job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}