package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod
import org.apache.log4j.Logger

class SupervisorManager(
    private val logger: Logger,
    private val cache: Cache,
    private val controller: OperatorController,
    private val cluster: V1FlinkCluster
) {
    fun reconcile() {
        val clusterName = getName(cluster)

        val supervisorResources = cache.getSupervisorResources(clusterName)

        if (supervisorResources.supervisorDep != null) {
            if (controller.isClusterTerminated(cluster)) {
                if (supervisorResources.supervisorDep.metadata?.deletionTimestamp != null) {
                    logger.info("Supervisor deployment deleted. Awaiting termination...")
                } else if (controller.timeSinceLastUpdateInSeconds(cluster) > 30) {
                    logger.info("Cluster terminated. Deleting supervisor deployment...")
                    controller.deleteDeployment(cache.namespace, clusterName, getName(supervisorResources.supervisorDep))
                }
            } else {
                if (supervisorResources.supervisorDep.metadata?.deletionTimestamp != null) {
                    logger.info("Supervisor deployment deleted. Awaiting termination...")
                } else if (controller.hasSupervisorChanged(supervisorResources.supervisorDep, cluster)) {
                    logger.info("Supervisor modified. Deleting supervisor deployment...")
                    controller.deleteDeployment(cache.namespace, clusterName, getName(supervisorResources.supervisorDep))
                }
            }
        }

        if (supervisorResources.supervisorDep == null && supervisorResources.supervisorPod == null) {
            val hasFinalizer = controller.hasFinalizer(cluster)

            if (!controller.isClusterTerminated(cluster)) {
                if (!hasFinalizer) {
                    logger.info("Add finalizer: cluster ${cluster.metadata.name}")
                    controller.addFinalizer(cluster)
                } else {
                    logger.info("Supervisor not found. Creating supervisor deployment...")
                    controller.createSupervisorDeployment(cache.namespace, clusterName, cluster)
                }
            } else {
                logger.info("Remove finalizer: cluster ${cluster.metadata.name}")
                controller.removeFinalizer(cluster)
            }

            val newHasFinalizer = controller.hasFinalizer(cluster)

            if (hasFinalizer != newHasFinalizer) {
                logger.debug("Updating finalizers: cluster ${cluster.metadata.name}")
                controller.updateFinalizers(cache.namespace, clusterName, cluster)
            }
        }

        if (supervisorResources.supervisorDep == null && supervisorResources.supervisorPod != null) {
            if (supervisorResources.supervisorPod.metadata?.deletionTimestamp != null) {
                logger.info("Supervisor pod deleted. Awaiting termination...")
            } else {
                logger.info("Supervisor deployment missing. Deleting pod...")
                controller.deletePod(cache.namespace, clusterName, getName(supervisorResources.supervisorPod))
            }
        }

        if (supervisorResources.supervisorDep != null && supervisorResources.supervisorPod == null) {
            logger.warn("Supervisor pod not running")
        }
    }

    private fun getName(pod: V1Pod) =
        pod.metadata?.name ?: throw RuntimeException("Metadata name is null")

    private fun getName(job: V1Deployment) =
        job.metadata?.name ?: throw RuntimeException("Metadata name is null")

    private fun getName(cluster: V1FlinkCluster) =
        cluster.metadata?.name ?: throw RuntimeException("Metadata name is null")
}
