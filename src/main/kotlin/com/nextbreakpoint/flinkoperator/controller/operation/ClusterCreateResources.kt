package com.nextbreakpoint.flinkoperator.controller.operation

import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.common.utils.FlinkContext
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import com.nextbreakpoint.flinkoperator.controller.core.Operation
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import org.apache.log4j.Logger

class ClusterCreateResources(flinkOptions: FlinkOptions, flinkContext: FlinkContext, kubernetesContext: KubernetesContext) : Operation<ClusterResources, Void?>(flinkOptions, flinkContext, kubernetesContext) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreateResources::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): Result<Void?> {
        try {
            val services = kubernetesContext.listJobManagerServices(clusterId)

            val jobmanagerStatefulSets = kubernetesContext.listJobManagerStatefulSets(clusterId)

            val taskmanagerStatefulSets = kubernetesContext.listTaskManagerStatefulSets(clusterId)

            if (services.items.isNotEmpty() || jobmanagerStatefulSets.items.isNotEmpty() || taskmanagerStatefulSets.items.isNotEmpty()) {
                throw RuntimeException("Previous resources already exist")
            }

            logger.info("Creating resources of cluster ${clusterId.name}...")

            val jobmanagerServiceOut = kubernetesContext.createJobManagerService(clusterId, params)

            logger.info("Service created ${jobmanagerServiceOut.metadata.name}")

            val jobmanagerStatefulSetOut = kubernetesContext.createJobManagerStatefulSet(clusterId, params)

            logger.info("JobManager created ${jobmanagerStatefulSetOut.metadata.name}")

            val taskmanagerStatefulSetOut = kubernetesContext.createTaskManagerStatefulSet(clusterId, params)

            logger.info("TaskManager created ${taskmanagerStatefulSetOut.metadata.name}")

            return Result(
                ResultStatus.SUCCESS,
                null
            )
        } catch (e : Exception) {
            logger.error("Can't create resources of cluster ${clusterId.name}", e)

            return Result(
                ResultStatus.FAILED,
                null
            )
        }
    }
}