package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1Pod
import java.util.concurrent.ConcurrentHashMap

class OperatorCache {
    private val flinkClusters = ConcurrentHashMap<ClusterSelector, V1FlinkCluster>()
    private val supervisorPods = ConcurrentHashMap<ClusterSelector, V1Pod>()
    private val supervisorDeployments = ConcurrentHashMap<ClusterSelector, V1Deployment>()

    fun getClusterSelectors(): List<ClusterSelector> = flinkClusters.keys.toList()

    fun findClusterSelector(namespace: String, name: String) =
            flinkClusters.keys.firstOrNull {
                it.namespace == namespace && it.name == name
            } ?: throw RuntimeException("Cluster not found")

    fun getFlinkClusters(): List<V1FlinkCluster> = flinkClusters.values.toList()

    fun getFlinkCluster(clusterSelector: ClusterSelector) = flinkClusters[clusterSelector]
            ?: throw RuntimeException("Cluster not found ${clusterSelector.name}")

    fun getCachedResources(clusterSelector: ClusterSelector) =
            OperatorCachedResources(
                flinkCluster = flinkClusters[clusterSelector],
                supervisorPod = supervisorPods[clusterSelector],
                supervisorDeployment = supervisorDeployments[clusterSelector]
            )

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        val clusterSelector = ClusterSelector(
                namespace = resource.metadata.namespace,
                name = resource.metadata.name,
                uuid = resource.metadata.uid
        )

        flinkClusters[clusterSelector] = resource
    }

    fun onFlinkClusterDeleted(resource: V1FlinkCluster) {
        val clusterSelector = ClusterSelector(
                namespace = resource.metadata.namespace,
                name = resource.metadata.name,
                uuid = resource.metadata.uid
        )

        flinkClusters.remove(clusterSelector)
    }

    fun onFlinkClusterDeletedAll() {
        flinkClusters.clear()
    }

    fun onPodChanged(resource: V1Pod) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "supervisor" -> {
                supervisorPods[clusterSelector] = resource
            }
        }
    }

    fun onPodDeleted(resource: V1Pod) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "supervisor" -> {
                supervisorPods.remove(clusterSelector)
            }
        }
    }

    fun onPodDeletedAll() {
        supervisorPods.clear()
    }

    fun onDeploymentChanged(resource: V1Deployment) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "supervisor" -> {
                supervisorDeployments[clusterSelector] = resource
            }
        }
    }

    fun onDeploymentDeleted(resource: V1Deployment) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "supervisor" -> {
                supervisorDeployments.remove(clusterSelector)
            }
        }
    }

    fun onDeploymentDeletedAll() {
        supervisorDeployments.clear()
    }

    private fun makeClusterSelector(metadata: V1ObjectMeta) =
            ClusterSelector(
                namespace = metadata.namespace,
                name = extractClusterName(metadata),
                uuid = extractClusterSelector(metadata)
            )

    private fun extractClusterName(objectMeta: V1ObjectMeta) =
            objectMeta.labels?.get("name") ?: throw RuntimeException("Missing required label name")

    private fun extractClusterSelector(objectMeta: V1ObjectMeta) =
            objectMeta.labels?.get("uid") ?: throw RuntimeException("Missing required label uid")
}