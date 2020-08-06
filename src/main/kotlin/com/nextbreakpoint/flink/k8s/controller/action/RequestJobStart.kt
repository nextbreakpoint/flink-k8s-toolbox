package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.common.ManualAction
import com.nextbreakpoint.flink.common.StartOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import org.apache.log4j.Logger

class RequestJobStart(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: JobContext) : Action<StartOptions, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(RequestJobStart::class.simpleName)
    }

    override fun execute(jobSelector: ResourceSelector, params: StartOptions): Result<Void?> {
        return try {
            context.setJobWithoutSavepoint(params.withoutSavepoint)
            context.setJobManualAction(ManualAction.START)

            kubeClient.updateJobAnnotations(jobSelector, context.getJobAnnotations())

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't start job", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}