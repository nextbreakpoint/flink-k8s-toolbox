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
    private val logger = Logger.getLogger(ClusterCreateResources::class.simpleName)

    override fun execute(clusterId: ClusterId, resources: ClusterResources): Result<Void?> {
        try {
            logger.info("Creating resources of cluster ${clusterId.name}...")

            val statefulSets = Kubernetes.appsApi.listNamespacedStatefulSet(
                clusterId.namespace,
                null,
                null,
                null,
                null,
                "name=${clusterId.name},owner=flink-operator",
                null,
                null,
                30,
                null
            )

            if (statefulSets.items.size > 0) {
                throw RuntimeException("Cluster already exists")
            }

            logger.info("Creating Flink Service ...")

            val jobmanagerServiceOut = Kubernetes.coreApi.createNamespacedService(
                clusterId.namespace,
                resources.jobmanagerService,
                null,
                null,
                null
            )

            logger.info("Service created ${jobmanagerServiceOut.metadata.name}")

            logger.info("Creating JobManager StatefulSet ...")

            val jobmanagerStatefulSetOut = Kubernetes.appsApi.createNamespacedStatefulSet(
                clusterId.namespace,
                resources.jobmanagerStatefulSet,
                null,
                null,
                null
            )

            logger.info("StatefulSet created ${jobmanagerStatefulSetOut.metadata.name}")

            logger.info("Creating TaskManager StatefulSet ...")

            val taskmanagerStatefulSetOut = Kubernetes.appsApi.createNamespacedStatefulSet(
                clusterId.namespace,
                resources.taskmanagerStatefulSet,
                null,
                null,
                null
            )

            logger.info("StatefulSet created ${taskmanagerStatefulSetOut.metadata.name}")

            return Result(ResultStatus.SUCCESS, null)
        } catch (e : Exception) {
            logger.error("Can't create resources of cluster ${clusterId.name}", e)

            return Result(ResultStatus.FAILED, null)
        }
    }
}