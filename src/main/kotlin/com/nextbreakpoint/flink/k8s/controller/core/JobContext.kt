package com.nextbreakpoint.flink.k8s.controller.core

import com.nextbreakpoint.flink.common.Action
import com.nextbreakpoint.flink.k8s.common.FlinkJobAnnotations
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob

class JobContext(private val job: V1FlinkJob) {
    fun setJobWithoutSavepoint(withoutSavepoint: Boolean) {
        FlinkJobAnnotations.setWithoutSavepoint(job, withoutSavepoint)
    }

    fun setJobManualAction(action: Action) {
        FlinkJobAnnotations.setRequestedAction(job, action)
    }

    // the returned map must be immutable to avoid side effects
    fun getJobAnnotations() = job.metadata?.annotations?.toMap().orEmpty()

    // we should make copy of status to avoid side effects
    fun getJobStatus() = job.status

    fun getJobId() = job.status.jobId
}