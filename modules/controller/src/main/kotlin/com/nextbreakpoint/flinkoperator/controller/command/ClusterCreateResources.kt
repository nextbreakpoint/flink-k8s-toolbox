package com.nextbreakpoint.flinkoperator.controller.command

import com.nextbreakpoint.flinkoperator.common.utils.KubernetesUtils
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.FlinkOptions
import com.nextbreakpoint.flinkoperator.common.model.Result
import com.nextbreakpoint.flinkoperator.common.model.ResultStatus
import com.nextbreakpoint.flinkoperator.controller.OperatorCommand
import com.nextbreakpoint.flinkoperator.controller.resources.ClusterResources
import org.apache.log4j.Logger

class ClusterCreateResources(flinkOptions: FlinkOptions) : OperatorCommand<ClusterResources, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreateResources::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, params: ClusterResources): Result<Void?> {
        try {
            val services = KubernetesUtils.coreApi.listNamespacedService(
                clusterId.namespace,
                null,
                null,
                null,
                null,
                "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator",
                null,
                null,
                30,
                null
            )

            val jobmanagerStatefulSets = KubernetesUtils.appsApi.listNamespacedStatefulSet(
                clusterId.namespace,
                null,
                null,
                null,
                null,
                "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=jobmanager",
                null,
                null,
                30,
                null
            )

            val taskmanagerStatefulSets = KubernetesUtils.appsApi.listNamespacedStatefulSet(
                clusterId.namespace,
                null,
                null,
                null,
                null,
                "name=${clusterId.name},uid=${clusterId.uuid},owner=flink-operator,role=taskmanager",
                null,
                null,
                30,
                null
            )

            if (services.items.isNotEmpty() || jobmanagerStatefulSets.items.isNotEmpty() || taskmanagerStatefulSets.items.isNotEmpty()) {
                throw RuntimeException("Previous resources already exist")
            }

            logger.info("Creating resources of cluster ${clusterId.name}...")

            val jobmanagerServiceOut = KubernetesUtils.coreApi.createNamespacedService(
                clusterId.namespace,
                params.jobmanagerService,
                null,
                null,
                null
            )

            logger.info("Service created ${jobmanagerServiceOut.metadata.name}")

            val jobmanagerStatefulSetOut = KubernetesUtils.appsApi.createNamespacedStatefulSet(
                clusterId.namespace,
                params.jobmanagerStatefulSet,
                null,
                null,
                null
            )

            logger.info("JobManager created ${jobmanagerStatefulSetOut.metadata.name}")

            val taskmanagerStatefulSetOut = KubernetesUtils.appsApi.createNamespacedStatefulSet(
                clusterId.namespace,
                params.taskmanagerStatefulSet,
                null,
                null,
                null
            )

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