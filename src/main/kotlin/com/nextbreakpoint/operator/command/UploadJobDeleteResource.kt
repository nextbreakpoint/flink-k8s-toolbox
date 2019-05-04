package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import io.kubernetes.client.apis.BatchV1Api
import io.kubernetes.client.models.V1DeleteOptions
import org.apache.log4j.Logger

class UploadJobDeleteResource(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    private val logger = Logger.getLogger(UploadJobDeleteResource::class.simpleName)

    private val deleteOptions = V1DeleteOptions().propagationPolicy("Background")

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            logger.info("Deleting upload job of cluster ${clusterId.name}...")

            deleteJob(Kubernetes.batchApi, "flink-operator", clusterId)

            return Result(ResultStatus.SUCCESS, null)
        } catch (e : Exception) {
            logger.error("Can't delete upload job of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }

    private fun deleteJob(api: BatchV1Api, owner: String, clusterId: ClusterId) {
        val jobs = api.listNamespacedJob(
            clusterId.namespace,
            null,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=$owner",
            null,
            null,
            30,
            null
        )

        jobs.items.forEach { job ->
            try {
                logger.info("Removing Job ${job.metadata.name}...")

                val status = api.deleteNamespacedJob(
                    job.metadata.name,
                    clusterId.namespace,
                    deleteOptions,
                    null,
                    null,
                    5,
                    null,
                    null
                )

                logger.info("Response status: ${status.reason}")

                status.details.causes.forEach { logger.info(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }
}