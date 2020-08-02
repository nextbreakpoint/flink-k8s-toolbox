package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ClusterStatus
import com.nextbreakpoint.flinkoperator.common.model.DeleteOptions
import com.nextbreakpoint.flinkoperator.common.utils.ClusterResource
import com.nextbreakpoint.flinkoperator.controller.resources.SupervisorResourcesDefaultFactory
import org.apache.log4j.Logger

class Operator(
    private val controller: OperationController,
    private val logger: Logger
) {
    companion object {
        fun create(controller: OperationController, loggerName: String): Operator {
            val logger = Logger.getLogger(loggerName)
            return Operator(controller, logger)
        }
    }

    fun reconcile(clusterSelector: ClusterSelector, resources: OperatorCachedResources) {
        val cluster = resources.flinkCluster ?: throw RuntimeException("Cluster not present")

        if (Status.getClusterStatus(cluster) == ClusterStatus.Terminated) {
            val lastUpdateTimestamp = Status.getStatusTimestamp(cluster).toInstant().millis

            if (System.currentTimeMillis() - lastUpdateTimestamp > 30000) {
                if (resources.supervisorDeployment != null) {
                    logger.info("Cluster is terminated. Delete supervisor")

                    controller.deleteSupervisorDeployment(clusterSelector)
                } else {
                    if (resources.supervisorPod == null) {
                        if (cluster.metadata.deletionTimestamp != null) {
                            logger.info("Cluster is terminated. Remove finalizer")

                            removeFinalizer(cluster)

                            controller.updateFinalizers(clusterSelector, cluster)
                        } else {
                            logger.info("Cluster is terminated. Delete cluster")

                            controller.deleteFlinkCluster(clusterSelector)
                        }
                    } else {
                        if (resources.supervisorPod.metadata.deletionTimestamp != null) {
                            logger.warn("Supervisor pod deleted. Await termination")
                        } else {
                            logger.warn("Found supervisor pod. Terminate pod")

                            controller.deletePods(clusterSelector, DeleteOptions("role", "supervisor", 1))
                        }
                    }
                }
            } else {
                logger.info("Cluster is terminated")
            }
        } else {
            if (resources.supervisorDeployment == null) {
                if (resources.supervisorPod == null) {
                    logger.info("Create supervisor")

                    val deployment = SupervisorResourcesDefaultFactory.createSupervisorDeployment(
                        clusterSelector, "flink-operator", cluster.spec.operator
                    )

                    controller.createSupervisorDeployment(clusterSelector, deployment)
                } else {
                    if (resources.supervisorPod.metadata.deletionTimestamp != null) {
                        logger.warn("Supervisor pod deleted. Await termination")
                    } else {
                        logger.warn("Found supervisor pod. Terminate pod")

                        controller.deletePods(clusterSelector, DeleteOptions("role", "supervisor", 1))
                    }
                }
            } else {
                val deployedDigest = resources.supervisorDeployment.metadata.annotations["flink-operator/deployment-digest"]

                val declaredDigest = ClusterResource.computeDigest(cluster.spec.operator)

                if (deployedDigest == null || deployedDigest != declaredDigest) {
                    logger.info("Detected change. Recreate supervisor")

                    controller.deleteSupervisorDeployment(clusterSelector)
                }
            }

            if (resources.supervisorDeployment != null && resources.supervisorPod == null) {
                logger.warn("Supervisor pod not found")
            }
        }
    }

    private fun removeFinalizer(cluster: V1FlinkCluster) {
        if (cluster.metadata.finalizers != null && cluster.metadata.finalizers.contains("finalizer.nextbreakpoint.com")) {
            cluster.metadata.finalizers = cluster.metadata.finalizers.minus("finalizer.nextbreakpoint.com")
        }
    }
}
