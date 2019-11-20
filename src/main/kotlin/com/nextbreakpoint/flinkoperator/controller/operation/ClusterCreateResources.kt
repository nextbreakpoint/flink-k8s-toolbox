package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkClient
import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import org.apache.log4j.Logger

class ClusterCreateResources(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<ClusterResources, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreateResources::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): Result<Void?> {
        try {
            val services = kubeClient.listJobManagerServices(clusterId)

            val jobmanagerStatefulSets = kubeClient.listJobManagerStatefulSets(clusterId)

            val taskmanagerStatefulSets = kubeClient.listTaskManagerStatefulSets(clusterId)

            if (services.items.isNotEmpty() || jobmanagerStatefulSets.items.isNotEmpty() || taskmanagerStatefulSets.items.isNotEmpty()) {
                throw RuntimeException("Previous resources already exist")
            }

            logger.info("[name=${clusterId.name}] Creating resources...")

            val jobmanagerServiceOut = kubeClient.createJobManagerService(clusterId, params)

            logger.info("[name=${clusterId.name}] Service created ${jobmanagerServiceOut.metadata.name}")

            val jobmanagerStatefulSetOut = kubeClient.createJobManagerStatefulSet(clusterId, params)

            logger.info("[name=${clusterId.name}] JobManager created ${jobmanagerStatefulSetOut.metadata.name}")

            val taskmanagerStatefulSetOut = kubeClient.createTaskManagerStatefulSet(clusterId, params)

            logger.info("[name=${clusterId.name}] TaskManager created ${taskmanagerStatefulSetOut.metadata.name}")

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't create resources", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}