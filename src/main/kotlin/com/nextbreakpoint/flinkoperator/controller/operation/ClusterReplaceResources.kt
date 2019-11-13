package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import org.apache.log4j.Logger

class ClusterReplaceResources(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : OperatorCommand<ClusterResources, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterReplaceResources::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): Result<Void?> {
        try {
            val jobmanagerStatefulSets = kubernetesContext.listJobManagerStatefulSets(clusterId)

            val taskmanagerStatefulSets = kubernetesContext.listTaskManagerStatefulSets(clusterId)

            if (jobmanagerStatefulSets.items.isEmpty() || taskmanagerStatefulSets.items.isEmpty()) {
                throw RuntimeException("Previous resources don't exist")
            }

            logger.info("Replacing resources of cluster ${clusterId.name}...")

            kubernetesContext.deleteServices(clusterId)

            val jobmanagerServiceOut = kubernetesContext.createJobManagerService(clusterId, params)

            logger.info("Service replaced ${jobmanagerServiceOut.metadata.name}")

            val jobmanagerStatefulSetOut = kubernetesContext.replaceJobManagerStatefulSet(clusterId, params)

            logger.info("JobManager replaced ${jobmanagerStatefulSetOut.metadata.name}")

            val taskmanagerStatefulSetOut = kubernetesContext.replaceTaskManagerStatefulSet(clusterId, params)

            logger.info("TaskManager replaced ${taskmanagerStatefulSetOut.metadata.name}")

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't replace resources of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}