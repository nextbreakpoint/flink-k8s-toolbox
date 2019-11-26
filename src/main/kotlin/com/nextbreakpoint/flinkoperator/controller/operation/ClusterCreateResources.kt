package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import org.apache.log4j.Logger

class ClusterCreateResources(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<ClusterResources, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreateResources::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): OperationResult<Void?> {
        try {
            logger.info("[name=${clusterId.name}] Creating resources...")

            val services = kubeClient.listJobManagerServices(clusterId)

            val jobmanagerStatefulSets = kubeClient.listJobManagerStatefulSets(clusterId)

            val taskmanagerStatefulSets = kubeClient.listTaskManagerStatefulSets(clusterId)

            if (services.items.isNotEmpty()) {
                kubeClient.deleteJobManagerServices(clusterId)

                val jobmanagerServiceOut = kubeClient.createJobManagerService(clusterId, params)

                logger.info("[name=${clusterId.name}] Service recreated: ${jobmanagerServiceOut.metadata.name}")
            } else {
                val jobmanagerServiceOut = kubeClient.createJobManagerService(clusterId, params)

                logger.info("[name=${clusterId.name}] Service created: ${jobmanagerServiceOut.metadata.name}")
            }

            if (jobmanagerStatefulSets.items.isNotEmpty()) {
                params.jobmanagerStatefulSet?.apiVersion = jobmanagerStatefulSets.items.first()?.apiVersion
                params.jobmanagerStatefulSet?.kind = jobmanagerStatefulSets.items.first()?.kind
                params.jobmanagerStatefulSet?.metadata = jobmanagerStatefulSets.items.first()?.metadata

                val jobmanagerStatefulSetOut = kubeClient.replaceJobManagerStatefulSet(clusterId, params)

                logger.info("[name=${clusterId.name}] JobManager replaced: ${jobmanagerStatefulSetOut.metadata.name}")
            } else {
                val jobmanagerStatefulSetOut = kubeClient.createJobManagerStatefulSet(clusterId, params)

                logger.info("[name=${clusterId.name}] JobManager created: ${jobmanagerStatefulSetOut.metadata.name}")
            }

            if (taskmanagerStatefulSets.items.isNotEmpty()) {
                params.taskmanagerStatefulSet?.apiVersion = taskmanagerStatefulSets.items.first()?.apiVersion
                params.taskmanagerStatefulSet?.kind = taskmanagerStatefulSets.items.first()?.kind
                params.taskmanagerStatefulSet?.metadata = taskmanagerStatefulSets.items.first()?.metadata

                val taskmanagerStatefulSetOut = kubeClient.replaceTaskManagerStatefulSet(clusterId, params)

                logger.info("[name=${clusterId.name}] TaskManager replaced: ${taskmanagerStatefulSetOut.metadata.name}")
            } else {
                val taskmanagerStatefulSetOut = kubeClient.createTaskManagerStatefulSet(clusterId, params)

                logger.info("[name=${clusterId.name}] TaskManager created: ${taskmanagerStatefulSetOut.metadata.name}")
            }

            return OperationResult(
                OperationStatus.COMPLETED,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't create resources", e)

            return OperationResult(
                OperationStatus.FAILED,
                null
            )
        }
    }
}