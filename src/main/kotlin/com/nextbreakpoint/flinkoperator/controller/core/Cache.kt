package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import java.util.concurrent.ConcurrentHashMap

class Cache {
    private val clusters = ConcurrentHashMap<ClusterId, V1FlinkCluster>()
    private val resources = ConcurrentHashMap<ClusterId, CachedResources>()

    fun getFlinkClusters(): List<V1FlinkCluster> = clusters.values.toList()

    fun getCachedClusters(): List<ClusterId> = clusters.keys.toList()

    fun getFlinkCluster(clusterId: ClusterId) = clusters[clusterId] ?: throw RuntimeException("Cluster not found ${clusterId.name}")

    fun getClusterId(namespace: String, name: String) =
        clusters.keys.firstOrNull { it.namespace == namespace && it.name == name } ?: throw RuntimeException("Cluster not found")

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        val clusterId = ClusterId(
            namespace = resource.metadata.namespace,
            name = resource.metadata.name,
            uuid = resource.metadata.uid
        )

        clusters[clusterId] = resource
    }

    fun onFlinkClusterDeleted(resource: V1FlinkCluster) {
        val clusterId = ClusterId(
            namespace = resource.metadata.namespace,
            name = resource.metadata.name,
            uuid = resource.metadata.uid
        )

        clusters.remove(clusterId)
    }

    fun onFlinkClusterDeleteAll() {
        clusters.clear()
    }
}
