package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import io.kubernetes.client.models.*
import java.util.concurrent.ConcurrentHashMap

class Cache {
    private val flinkClusters = ConcurrentHashMap<ClusterId, V1FlinkCluster>()
    private val bootstrapJobs = ConcurrentHashMap<ClusterId, V1Job>()
    private val jobmanagerServices = ConcurrentHashMap<ClusterId, V1Service>()
    private val jobmanagerStatefulSets = ConcurrentHashMap<ClusterId, V1StatefulSet>()
    private val taskmanagerStatefulSets = ConcurrentHashMap<ClusterId, V1StatefulSet>()
    private val jobmanagerPersistentVolumeClaims = ConcurrentHashMap<ClusterId, V1PersistentVolumeClaim>()
    private val taskmanagerPersistentVolumeClaims = ConcurrentHashMap<ClusterId, V1PersistentVolumeClaim>()

    fun getClusterIds(): List<ClusterId> = flinkClusters.keys.toList()

    fun getClusterId(namespace: String, name: String) =
            flinkClusters.keys.firstOrNull {
                it.namespace == namespace && it.name == name
            } ?: throw RuntimeException("Cluster not found")

    fun getFlinkClusters(): List<V1FlinkCluster> = flinkClusters.values.toList()

    fun getFlinkCluster(clusterId: ClusterId) = flinkClusters[clusterId]
            ?: throw RuntimeException("Cluster not found ${clusterId.name}")

    fun getCachedResources(clusterId: ClusterId) =
            CachedResources(
                    flinkCluter = flinkClusters[clusterId],
                    bootstrapJob = bootstrapJobs[clusterId],
                    jobmanagerService = jobmanagerServices[clusterId],
                    jobmanagerStatefulSet = jobmanagerStatefulSets[clusterId],
                    taskmanagerStatefulSet = taskmanagerStatefulSets[clusterId],
                    jobmanagerPVC = jobmanagerPersistentVolumeClaims[clusterId],
                    taskmanagerPVC = taskmanagerPersistentVolumeClaims[clusterId]
            )

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

    fun onServiceChanged(resource: V1Service) {
        val clusterId = makeClusterId(resource.metadata)

        jobmanagerServices[clusterId] = resource
    }

    fun onServiceDeleted(resource: V1Service) {
        val clusterId = makeClusterId(resource.metadata)

        jobmanagerServices.remove(clusterId)
    }

    fun onJobChanged(resource: V1Job) {
        val clusterId = makeClusterId(resource.metadata)

        bootstrapJobs[clusterId] = resource
    }

    fun onJobDeleted(resource: V1Job) {
        val clusterId = makeClusterId(resource.metadata)

        bootstrapJobs.remove(clusterId)
    }

    fun onStatefulSetChanged(resource: V1StatefulSet) {
        val clusterId = makeClusterId(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerStatefulSets[clusterId] = resource

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerStatefulSets[clusterId] = resource
        }
    }

    fun onStatefulSetDeleted(resource: V1StatefulSet) {
        val clusterId = makeClusterId(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerStatefulSets.remove(clusterId)

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerStatefulSets.remove(clusterId)
        }
    }

    fun onPersistentVolumeClaimChanged(resource: V1PersistentVolumeClaim) {
        val clusterId = makeClusterId(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerPersistentVolumeClaims[clusterId] = resource

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerPersistentVolumeClaims[clusterId] = resource
        }
    }

    fun onPersistentVolumeClaimDeleted(resource: V1PersistentVolumeClaim) {
        val clusterId = makeClusterId(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerPersistentVolumeClaims.remove(clusterId)

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerPersistentVolumeClaims.remove(clusterId)
        }
    }

    fun onJobDeleteAll() {
        bootstrapJobs.clear()
    }

    fun onServiceDeletedAll() {
        jobmanagerServices.clear()
    }

    fun onStatefulSetDeletedAll() {
        jobmanagerStatefulSets.clear()
        taskmanagerStatefulSets.clear()
    }

    fun onPersistentVolumeClaimDeletedAll() {
        jobmanagerPersistentVolumeClaims.clear()
        taskmanagerPersistentVolumeClaims.clear()
    }

    private fun makeClusterId(metadata: V1ObjectMeta) =
            ClusterId(
                namespace = metadata.namespace,
                name = extractClusterName(metadata),
                uuid = extractClusterId(metadata)
            )

    private fun extractClusterName(objectMeta: V1ObjectMeta) =
            objectMeta.labels?.get("name") ?: throw RuntimeException("Missing required label name")

    private fun extractClusterId(objectMeta: V1ObjectMeta) =
            objectMeta.labels?.get("uid") ?: throw RuntimeException("Missing required label uid")
}