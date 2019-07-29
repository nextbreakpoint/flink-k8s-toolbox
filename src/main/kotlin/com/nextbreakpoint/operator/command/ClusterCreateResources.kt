package com.nextbreakpoint.operator.command

import com.nextbreakpoint.common.Kubernetes
import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.operator.OperatorCommand
import com.nextbreakpoint.operator.resources.ClusterResources
import org.apache.log4j.Logger

class ClusterCreateResources(flinkOptions: FlinkOptions) : OperatorCommand<ClusterResources, Void?>(flinkOptions) {
    companion object {
        private val logger = Logger.getLogger(ClusterCreateResources::class.simpleName)
    }

    override fun execute(clusterId: ClusterId, resources: ClusterResources): Result<Void?> {
        try {
            val services = Kubernetes.coreApi.listNamespacedService(
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

            val jobmanagerStatefulSets = Kubernetes.appsApi.listNamespacedStatefulSet(
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

            val taskmanagerStatefulSets = Kubernetes.appsApi.listNamespacedStatefulSet(
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

            val jobmanagerServiceOut = Kubernetes.coreApi.createNamespacedService(
                clusterId.namespace,
                resources.jobmanagerService,
                null,
                null,
                null
            )

            logger.info("Service created ${jobmanagerServiceOut.metadata.name}")

            val jobmanagerStatefulSetOut = Kubernetes.appsApi.createNamespacedStatefulSet(
                clusterId.namespace,
                resources.jobmanagerStatefulSet,
                null,
                null,
                null
            )

            logger.info("JobManager created ${jobmanagerStatefulSetOut.metadata.name}")

            val taskmanagerStatefulSetOut = Kubernetes.appsApi.createNamespacedStatefulSet(
                clusterId.namespace,
                resources.taskmanagerStatefulSet,
                null,
                null,
                null
            )

            logger.info("TaskManager created ${taskmanagerStatefulSetOut.metadata.name}")

            return Result(ResultStatus.SUCCESS, null)
        } catch (e : Exception) {
            logger.error("Can't create resources of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }
}