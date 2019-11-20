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

class ClusterReplaceResources(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : Operation<ClusterResources, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(ClusterReplaceResources::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): Result<Void?> {
        try {
            val jobmanagerStatefulSets = kubeClient.listJobManagerStatefulSets(clusterId)

            val taskmanagerStatefulSets = kubeClient.listTaskManagerStatefulSets(clusterId)

            if (jobmanagerStatefulSets.items.isEmpty() || taskmanagerStatefulSets.items.isEmpty()) {
                throw RuntimeException("Previous resources don't exist")
            }

            logger.info("[name=${clusterId.name}] Replacing resources...")

            kubeClient.deleteServices(clusterId)

            val jobmanagerServiceOut = kubeClient.createJobManagerService(clusterId, params)

            logger.info("[name=${clusterId.name}] Service replaced ${jobmanagerServiceOut.metadata.name}")

            val jobmanagerStatefulSetOut = kubeClient.replaceJobManagerStatefulSet(clusterId, params)

            logger.info("[name=${clusterId.name}] JobManager replaced ${jobmanagerStatefulSetOut.metadata.name}")

            val taskmanagerStatefulSetOut = kubeClient.replaceTaskManagerStatefulSet(clusterId, params)

            logger.info("[name=${clusterId.name}] TaskManager replaced ${taskmanagerStatefulSetOut.metadata.name}")

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("[name=${clusterId.name}] Can't replace resources", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}