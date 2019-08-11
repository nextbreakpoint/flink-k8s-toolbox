package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.utils.KubernetesUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.BatchV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.models.V1DeleteOptions
import org.apache.log4j.Logger

class ClusterDeleteResources(flinkOptions: FlinkOptions) : OperatorCommand<Void?, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(ClusterDeleteResources::class.simpleName)

        private val deleteOptions = V1DeleteOptions().propagationPolicy("Background")
    }

    override fun execute(clusterId: ClusterId, params: Void?): Result<Void?> {
        try {
            logger.info("Deleting resources of cluster ${clusterId.name}...")

            deleteJob(KubernetesUtils.batchApi, "flink-operator", clusterId)

            deleteStatefulSets(KubernetesUtils.appsApi, "flink-operator", clusterId)

            deleteService(KubernetesUtils.coreApi, "flink-operator", clusterId)

            deletePersistentVolumeClaims(KubernetesUtils.coreApi, "flink-operator", clusterId)

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't delete resources of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }

    private fun deleteStatefulSets(api: AppsV1Api, owner: String, clusterId: ClusterId) {
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
                logger.info("Removing StatefulSet ${statefulSet.metadata.name}...")

                val status = api.deleteNamespacedStatefulSet(
                    statefulSet.metadata.name,
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

    private fun deleteService(coreApi: CoreV1Api, owner: String, clusterId: ClusterId) {
        val services = coreApi.listNamespacedService(
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

        services.items.forEach { service ->
            try {
                logger.info("Removing Service ${service.metadata.name}...")

                val status = coreApi.deleteNamespacedService(
                    service.metadata.name,
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

    private fun deletePersistentVolumeClaims(coreApi: CoreV1Api, owner: String, clusterId: ClusterId) {
        val volumeClaims = coreApi.listNamespacedPersistentVolumeClaim(
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

        volumeClaims.items.forEach { volumeClaim ->
            try {
                logger.info("Removing Persistent Volume Claim ${volumeClaim.metadata.name}...")

                val status = coreApi.deleteNamespacedPersistentVolumeClaim(
                    volumeClaim.metadata.name,
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