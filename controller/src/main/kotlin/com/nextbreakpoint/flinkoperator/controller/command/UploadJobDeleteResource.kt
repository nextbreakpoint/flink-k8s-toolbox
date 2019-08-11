package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.utils.KubernetesUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import io.kubernetes.client.apis.BatchV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.models.V1DeleteOptions
import org.apache.log4j.Logger

class UploadJobDeleteResource(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(UploadJobDeleteResource::class.simpleName)

        private val deleteOptions = V1DeleteOptions().propagationPolicy("Background")
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            logger.info("Deleting upload job of cluster ${clusterId.name}...")

            deleteJob(KubernetesUtils.batchApi, "flink-operator", clusterId)

            deletePod(KubernetesUtils.coreApi, "flink-operator", clusterId)

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't delete upload job of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }

    private fun deletePod(api: CoreV1Api, owner: String, clusterId: ClusterId) {
        val pods = api.listNamespacedPod(
            clusterId.namespace,
            null,
            null,
            null,
            null,
            "name=${clusterId.name},uid=${clusterId.uuid},owner=$owner,job-name=flink-upload-${clusterId.name}",
            null,
            null,
            30,
            null
        )

        pods.items.forEach { pod ->
            try {
                logger.info("Removing Job ${pod.metadata.name}...")

                val status = api.deleteNamespacedPod(
                    pod.metadata.name,
                    clusterId.namespace,
                    null,
                    deleteOptions,
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
                    null,
                    deleteOptions,
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