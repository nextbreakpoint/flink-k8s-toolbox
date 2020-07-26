package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import java.util.concurrent.ConcurrentHashMap

class Cache {
    private val flinkClusters = ConcurrentHashMap<ClusterSelector, V1FlinkCluster>()
    private val bootstrapJobs = ConcurrentHashMap<ClusterSelector, V1Job>()
    private val jobmanagerServices = ConcurrentHashMap<ClusterSelector, V1Service>()
    private val jobmanagerStatefulSets = ConcurrentHashMap<ClusterSelector, V1StatefulSet>()
    private val taskmanagerStatefulSets = ConcurrentHashMap<ClusterSelector, V1StatefulSet>()
    private val jobmanagerPersistentVolumeClaims = ConcurrentHashMap<ClusterSelector, V1PersistentVolumeClaim>()
    private val taskmanagerPersistentVolumeClaims = ConcurrentHashMap<ClusterSelector, V1PersistentVolumeClaim>()

    fun getClusterSelectors(): List<ClusterSelector> = flinkClusters.keys.toList()

    fun findClusterSelector(namespace: String, name: String) =
            flinkClusters.keys.firstOrNull {
                it.namespace == namespace && it.name == name
            } ?: throw RuntimeException("Cluster not found")

    fun getFlinkClusters(): List<V1FlinkCluster> = flinkClusters.values.toList()

    fun getFlinkCluster(clusterSelector: ClusterSelector) = flinkClusters[clusterSelector]
            ?: throw RuntimeException("Cluster not found ${clusterSelector.name}")

    fun getCachedResources(clusterSelector: ClusterSelector) =
            CachedResources(
                    flinkCluster = flinkClusters[clusterSelector],
                    bootstrapJob = bootstrapJobs[clusterSelector],
                    jobmanagerService = jobmanagerServices[clusterSelector],
                    jobmanagerStatefulSet = jobmanagerStatefulSets[clusterSelector],
                    taskmanagerStatefulSet = taskmanagerStatefulSets[clusterSelector],
                    jobmanagerPVC = jobmanagerPersistentVolumeClaims[clusterSelector],
                    taskmanagerPVC = taskmanagerPersistentVolumeClaims[clusterSelector]
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

    fun onServiceChanged(resource: V1Service) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        jobmanagerServices[clusterSelector] = resource
    }

    fun onServiceDeleted(resource: V1Service) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        jobmanagerServices.remove(clusterSelector)
    }

    fun onJobChanged(resource: V1Job) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        bootstrapJobs[clusterSelector] = resource
    }

    fun onJobDeleted(resource: V1Job) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        bootstrapJobs.remove(clusterSelector)
    }

    fun onStatefulSetChanged(resource: V1StatefulSet) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerStatefulSets[clusterSelector] = resource

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerStatefulSets[clusterSelector] = resource
        }
    }

    fun onStatefulSetDeleted(resource: V1StatefulSet) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerStatefulSets.remove(clusterSelector)

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerStatefulSets.remove(clusterSelector)
        }
    }

    fun onPersistentVolumeClaimChanged(resource: V1PersistentVolumeClaim) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerPersistentVolumeClaims[clusterSelector] = resource

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerPersistentVolumeClaims[clusterSelector] = resource
        }
    }

    fun onPersistentVolumeClaimDeleted(resource: V1PersistentVolumeClaim) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        when {
            resource.metadata.labels.get("role") == "jobmanager" ->
                jobmanagerPersistentVolumeClaims.remove(clusterSelector)

            resource.metadata.labels.get("role") == "taskmanager" ->
                taskmanagerPersistentVolumeClaims.remove(clusterSelector)
        }
    }

    fun onJobDeletedAll() {
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