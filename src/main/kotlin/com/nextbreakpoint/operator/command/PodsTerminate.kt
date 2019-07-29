package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.BatchV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.models.V1DeleteOptions
import org.apache.log4j.Logger

class PodsTerminate(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(PodsTerminate::class.simpleName)

        private val deleteOptions = V1DeleteOptions().propagationPolicy("Background")
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            logger.info("Terminating resources of cluster ${clusterId.name}...")

            terminateStatefulSets(Kubernetes.appsApi, "flink-operator", clusterId)

            deleteJob(Kubernetes.batchApi, "flink-operator", clusterId)

            deletePod(Kubernetes.coreApi, "flink-operator", clusterId)

            return Result(ResultStatus.SUCCESS, null)
        } catch (e : Exception) {
            logger.error("Can't terminate resources of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }

    private fun terminateStatefulSets(api: AppsV1Api, owner: String, clusterId: ClusterId) {
        val statefulSets = api.listNamespacedStatefulSet(
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

        statefulSets.items.forEach { statefulSet ->
            try {
                logger.info("Scaling StatefulSet ${statefulSet.metadata.name}...")

                val patch = listOf(
                    mapOf<String, Any?>(
                        "op" to "replace",
                        "path" to "/spec/replicas",
                        "value" to 0
                    )
                )

                val response = api.patchNamespacedStatefulSetScaleCall(
                    statefulSet.metadata.name,
                    clusterId.namespace,
                    patch,
                    null,
                    null,
                    null,
                    null
                ).execute()

                if (response.isSuccessful) {
                    logger.info("Scaled StatefulSet ${statefulSet.metadata.name}")
                } else {
                    logger.warn("Can't scale StatefulSet ${statefulSet.metadata.name}")
                }
            } catch (e: Exception) {
                logger.warn("Failed to scale StatefulSet ${statefulSet.metadata.name}", e)
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
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