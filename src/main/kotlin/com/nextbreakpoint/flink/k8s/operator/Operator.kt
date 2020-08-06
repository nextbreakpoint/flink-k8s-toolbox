package com.nextbreakpoint.flink.k8s.operator

import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.DeleteOptions
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.factory.SupervisorResourcesDefaultFactory
import com.nextbreakpoint.flink.k8s.operator.core.Cache
import org.apache.log4j.Logger

class Operator(
    private val controller: Controller,
    private val cache: Cache
) {
    companion object {
        fun create(controller: Controller, cache: Cache): Operator {
            return Operator(controller, cache)
        }
    }

    fun reconcile(clusterSelector: ResourceSelector) {
        val logger = Logger.getLogger(Operator::class.java.name + " | Cluster: " + clusterSelector.name)

        try {
            val clusterResources = cache.getCachedResources(clusterSelector)

            val cluster = clusterResources.flinkCluster ?: throw RuntimeException("Cluster not found")

            if (FlinkClusterStatus.getSupervisorStatus(cluster) == ClusterStatus.Terminated) {
                val lastUpdateTimestamp = FlinkClusterStatus.getStatusTimestamp(cluster).toInstant().millis

                if (System.currentTimeMillis() - lastUpdateTimestamp > 30000) {
                    if (clusterResources.supervisorDeployment != null) {
                        logger.info("Cluster is terminated. Delete supervisor")

                        controller.deleteSupervisorDeployment(clusterSelector)
                    } else {
                        if (clusterResources.supervisorPod == null) {
                            if (cluster.metadata.deletionTimestamp != null) {
                                logger.info("Cluster is terminated. Remove finalizer")

                                removeFinalizer(cluster)

                                controller.updateFinalizers(clusterSelector, cluster)
                            } else {
                                logger.info("Cluster is terminated. Delete cluster")

                                controller.deleteFlinkCluster(clusterSelector)
                            }
                        } else {
                            if (clusterResources.supervisorPod.metadata?.deletionTimestamp != null) {
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
                if (clusterResources.supervisorDeployment == null) {
                    if (clusterResources.supervisorPod == null) {
                        logger.info("Create supervisor")

                        val deployment = SupervisorResourcesDefaultFactory.createSupervisorDeployment(
                            clusterSelector, "flink-operator", cluster.spec.supervisor, 1, controller.isDryRun()
                        )

                        controller.createSupervisorDeployment(clusterSelector, deployment)
                    } else {
                        if (clusterResources.supervisorPod.metadata?.deletionTimestamp != null) {
                            logger.warn("Supervisor pod deleted. Await termination")
                        } else {
                            logger.warn("Found supervisor pod. Terminate pod")

                            controller.deletePods(clusterSelector, DeleteOptions("role", "supervisor", 1))
                        }
                    }
                } else {
                    val deployedDigest = clusterResources.supervisorDeployment.metadata?.annotations?.get("flink-operator/deployment-digest")

                    val declaredDigest = Resource.computeDigest(cluster.spec.supervisor)

                    if (deployedDigest == null || deployedDigest != declaredDigest) {
                        logger.info("Detected change. Recreate supervisor")

                        controller.deleteSupervisorDeployment(clusterSelector)
                    }
                }

                if (clusterResources.supervisorDeployment != null && clusterResources.supervisorPod == null) {
                    logger.warn("Supervisor pod not running")
                }
            }
        } catch (e: Exception) {
            logger.error("Error occurred while reconciling cluster $clusterSelector", e)
        }
    }

    private fun removeFinalizer(cluster: V2FlinkCluster) {
        val finalizers = cluster.metadata.finalizers
        if (finalizers != null && finalizers.contains("finalizer.nextbreakpoint.com")) {
            cluster.metadata.finalizers = finalizers.minus("finalizer.nextbreakpoint.com")
        }
    }
}
