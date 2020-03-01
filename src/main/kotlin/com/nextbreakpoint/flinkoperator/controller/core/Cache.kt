package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId

class Cache {
    private val flinkClusters = mutableMapOf<ClusterId, V1FlinkCluster>()

    fun getFlinkClusters(): List<V1FlinkCluster> = flinkClusters.values.toList()

    fun getCachedClusters(): List<ClusterId> = flinkClusters.keys.toList()

    fun getFlinkCluster(clusterId: ClusterId) = flinkClusters[clusterId] ?: throw RuntimeException("Cluster not found ${clusterId.name}")

    fun getClusterId(namespace: String, name: String) =
        flinkClusters.keys.firstOrNull { it.namespace == namespace && it.name == name } ?: throw RuntimeException("Cluster not found")

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        val clusterId = ClusterId(
            namespace = resource.metadata.namespace,
            name = resource.metadata.name,
            uuid = resource.metadata.uid
        )

        flinkClusters[clusterId] = resource
    }

    fun onFlinkClusterDeleted(resource: V1FlinkCluster) {
        val clusterId = ClusterId(
            namespace = resource.metadata.namespace,
            name = resource.metadata.name,
            uuid = resource.metadata.uid
        )

        flinkClusters.remove(clusterId)
    }

    fun onFlinkClusterDeleteAll() {
        flinkClusters.clear()
    }
}
