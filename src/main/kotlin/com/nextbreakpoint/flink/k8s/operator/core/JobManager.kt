package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.common.JobStatus
import com.nextbreakpoint.flink.common.ResourceStatus
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import java.util.logging.Logger

class JobManager(
    private val logger: Logger,
    private val cache: Cache,
    private val controller: OperatorController,
    private val job: V1FlinkJob,
) {
    fun reconcile() {
        val supervisorResources = matchClusterName(job)?.let {  cache.getSupervisorResources(it) }

        if (supervisorResources?.flinkCluster == null && supervisorResources?.supervisorDep == null) {
            val name = getName(job)

            FlinkJobStatus.setJobParallelism(job, 0)
            FlinkJobStatus.setJobStatus(job, "")
            FlinkJobStatus.setJobId(job, "")

            if (job.metadata?.deletionTimestamp != null) {
                logger.info("Job has been deleted: $name")
                FlinkJobStatus.setSupervisorStatus(job, JobStatus.Terminated)
                FlinkJobStatus.setResourceStatus(job, ResourceStatus.Updated)
            } else {
                FlinkJobStatus.setSupervisorStatus(job, JobStatus.Unknown)
                FlinkJobStatus.setResourceStatus(job, ResourceStatus.Unknown)
            }
        }
    }

    private fun matchClusterName(job: V1FlinkJob) =
        cache.listClusterNamesSnapshot().firstOrNull { job.metadata?.name?.startsWith("$it-") ?: false }

    private fun getName(job: V1FlinkJob) =
        job.metadata?.name ?: throw RuntimeException("Metadata name is null")
}
