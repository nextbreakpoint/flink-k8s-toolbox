package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

class OperatorCache {
    private val flinkClusters = mutableMapOf<ClusterId, V1FlinkCluster>()
    private val bootstrapJobs = mutableMapOf<ClusterId, V1Job>()
    private val jobmanagerServices = mutableMapOf<ClusterId, V1Service>()
    private val jobmanagerStatefulSets = mutableMapOf<ClusterId, V1StatefulSet>()
    private val taskmanagerStatefulSets = mutableMapOf<ClusterId, V1StatefulSet>()
    private val jobmanagerPersistentVolumeClaims = mutableMapOf<ClusterId, V1PersistentVolumeClaim>()
    private val taskmanagerPersistentVolumeClaims = mutableMapOf<ClusterId, V1PersistentVolumeClaim>()

    fun getFlinkClusters(): List<V1FlinkCluster> = flinkClusters.values.toList()

    fun getFlinkCluster(clusterId: ClusterId) = flinkClusters.get(clusterId) ?: throw RuntimeException("Cluster not found ${clusterId.name}")

    fun getClusterId(namespace: String, name: String) =
        flinkClusters.keys.firstOrNull { it.namespace == namespace && it.name == name } ?: throw RuntimeException("Cluster not found")

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        flinkClusters.put(
            ClusterId(
                namespace = resource.metadata.namespace,
                name = resource.metadata.name,
                uuid = resource.metadata.uid
            ), resource)
    }

    fun onFlinkClusterDeleted(resource: V1FlinkCluster) {
        flinkClusters.remove(
            ClusterId(
                namespace = resource.metadata.namespace,
                name = resource.metadata.name,
                uuid = resource.metadata.uid
            )
        )
    }

    fun onServiceChanged(resource: V1Service) {
        jobmanagerServices.put(
            ClusterId(
                namespace = resource.metadata.namespace,
                name = extractClusterName(resource.metadata),
                uuid = extractClusterId(resource.metadata)
            ), resource)
    }

    fun onServiceDeleted(resource: V1Service) {
        jobmanagerServices.remove(
            ClusterId(
                namespace = resource.metadata.namespace,
                name = extractClusterName(resource.metadata),
                uuid = extractClusterId(resource.metadata)
            )
        )
    }

    fun onJobChanged(resource: V1Job) {
        bootstrapJobs.put(
            ClusterId(
                namespace = resource.metadata.namespace,
                name = extractClusterName(resource.metadata),
                uuid = extractClusterId(resource.metadata)
            ), resource)
    }

    fun onJobDeleted(resource: V1Job) {
        bootstrapJobs.remove(
            ClusterId(
                namespace = resource.metadata.namespace,
                name = extractClusterName(resource.metadata),
                uuid = extractClusterId(resource.metadata)
            )
        )
    }

    fun onStatefulSetChanged(resource: V1StatefulSet) {
        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerStatefulSets.put(
                    ClusterId(
                        namespace = resource.metadata.namespace,
                        name = extractClusterName(resource.metadata),
                        uuid = extractClusterId(resource.metadata)
                    ), resource
                )

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerStatefulSets.put(
                    ClusterId(
                        namespace = resource.metadata.namespace,
                        name = extractClusterName(resource.metadata),
                        uuid = extractClusterId(resource.metadata)
                    ), resource
                )
        }
    }

    fun onStatefulSetDeleted(resource: V1StatefulSet) {
        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerStatefulSets.remove(
                    ClusterId(
                        namespace = resource.metadata.namespace,
                        name = extractClusterName(resource.metadata),
                        uuid = extractClusterId(resource.metadata)
                    )
                )

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerStatefulSets.remove(
                    ClusterId(
                        namespace = resource.metadata.namespace,
                        name = extractClusterName(resource.metadata),
                        uuid = extractClusterId(resource.metadata)
                    )
                )
        }
    }

    fun onPersistentVolumeClaimChanged(resource: V1PersistentVolumeClaim) {
        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerPersistentVolumeClaims.put(
                    ClusterId(
                        namespace = resource.metadata.namespace,
                        name = extractClusterName(resource.metadata),
                        uuid = extractClusterId(resource.metadata)
                    ), resource
                )

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerPersistentVolumeClaims.put(
                    ClusterId(
                        namespace = resource.metadata.namespace,
                        name = extractClusterName(resource.metadata),
                        uuid = extractClusterId(resource.metadata)
                    ), resource
                )
        }
    }

    fun onPersistentVolumeClaimDeleted(resource: V1PersistentVolumeClaim) {
        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerPersistentVolumeClaims.remove(
                    ClusterId(
                        namespace = resource.metadata.namespace,
                        name = extractClusterName(resource.metadata),
                        uuid = extractClusterId(resource.metadata)
                    )
                )

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerPersistentVolumeClaims.remove(
                    ClusterId(
                        namespace = resource.metadata.namespace,
                        name = extractClusterName(resource.metadata),
                        uuid = extractClusterId(resource.metadata)
                    )
                )
        }
    }

    fun getClusters(): List<Pair<V1FlinkCluster, OperatorResources>> {
        val resources = OperatorResources(
            bootstrapJobs.toMap(),
            jobmanagerServices.toMap(),
            jobmanagerStatefulSets.toMap(),
            taskmanagerStatefulSets.toMap(),
            jobmanagerPersistentVolumeClaims.toMap(),
            taskmanagerPersistentVolumeClaims.toMap()
        )
        return flinkClusters.values.map { flinkCluster -> flinkCluster to resources }.toList()
    }

    fun getResources(): OperatorResources {
        return OperatorResources(
            bootstrapJobs.toMap(),
            jobmanagerServices.toMap(),
            jobmanagerStatefulSets.toMap(),
            taskmanagerStatefulSets.toMap(),
            jobmanagerPersistentVolumeClaims.toMap(),
            taskmanagerPersistentVolumeClaims.toMap()
        )
    }

    fun getOrphanedClusters(): Set<ClusterId> {
        val deletedClusters = mutableSetOf<ClusterId>()
        deletedClusters.addAll(bootstrapJobs.filter { (clusterId, _) -> flinkClusters[clusterId] == null }.keys)
        deletedClusters.addAll(jobmanagerServices.filter { (clusterId, _) -> flinkClusters[clusterId] == null }.keys)
        deletedClusters.addAll(jobmanagerStatefulSets.filter { (clusterId, _) -> flinkClusters[clusterId] == null }.keys)
        deletedClusters.addAll(taskmanagerStatefulSets.filter { (clusterId, _) -> flinkClusters[clusterId] == null }.keys)
        deletedClusters.addAll(jobmanagerPersistentVolumeClaims.filter { (clusterId, _) -> flinkClusters[clusterId] == null }.keys)
        deletedClusters.addAll(taskmanagerPersistentVolumeClaims.filter { (clusterId, _) -> flinkClusters[clusterId] == null }.keys)
        return deletedClusters
    }

    fun onFlinkClusterDeleteAll() {
        flinkClusters.clear()
    }

    fun onJobDeleteAll() {
        bootstrapJobs.clear()
    }

    fun onServiceDeleteAll() {
        jobmanagerServices.clear()
    }

    fun onStatefulSetDeleteAll() {
        jobmanagerStatefulSets.clear()
        taskmanagerStatefulSets.clear()
    }

    fun onPersistentVolumeClaimDeleteAll() {
        jobmanagerPersistentVolumeClaims.clear()
        taskmanagerPersistentVolumeClaims.clear()
    }

    private fun extractClusterName(objectMeta: V1ObjectMeta) =
        objectMeta.labels?.get("name") ?: throw RuntimeException("Missing required label name")

    private fun extractClusterId(objectMeta: V1ObjectMeta) =
        objectMeta.labels?.get("uid") ?: throw RuntimeException("Missing required label uid")
}
