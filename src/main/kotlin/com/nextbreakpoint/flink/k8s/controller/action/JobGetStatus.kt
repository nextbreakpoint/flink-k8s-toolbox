package com.nextbreakpoint.flink.k8s.controller.action

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.FlinkOptions
import com.nextbreakpoint.flink.k8s.common.FlinkClient
import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.controller.core.JobContext
import com.nextbreakpoint.flink.k8s.controller.core.Action
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.controller.core.ResultStatus
import io.kubernetes.client.openapi.JSON

class JobGetStatus(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient, private val context: JobContext) : Action<Void?, String?>(flinkOptions, flinkClient, kubeClient) {
    override fun execute(jobSelector: ResourceSelector, params: Void?): Result<String?> {
        return Result(ResultStatus.OK, JSON().serialize(context.getJobStatus()))
    }
}