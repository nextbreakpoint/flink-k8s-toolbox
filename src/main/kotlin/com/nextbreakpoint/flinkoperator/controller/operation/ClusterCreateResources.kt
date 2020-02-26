package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.core.OperationResult
import com.nextbreakpoint.flinkoperator.controller.core.OperationStatus
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import org.apache.log4j.Logger

class ClusterCreateResources(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<ClusterResources, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreateResources::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): OperationResult<Void?> {
        try {
            logger.info("[name=${clusterId.name}] Creating resources...")

            val jobmanagerServiceOut = kubeClient.createJobManagerService(clusterId, params)

            logger.info("[name=${clusterId.name}] Service created: ${jobmanagerServiceOut.metadata.name}")

            val jobmanagerStatefulSetOut = kubeClient.createJobManagerStatefulSet(clusterId, params)

            logger.info("[name=${clusterId.name}] JobManager created: ${jobmanagerStatefulSetOut.metadata.name}")

            val taskmanagerStatefulSetOut = kubeClient.createTaskManagerStatefulSet(clusterId, params)

            logger.info("[name=${clusterId.name}] TaskManager created: ${taskmanagerStatefulSetOut.metadata.name}")

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