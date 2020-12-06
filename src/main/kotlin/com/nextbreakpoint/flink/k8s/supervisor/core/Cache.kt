package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service
import java.util.concurrent.ConcurrentHashMap

class Cache(val namespace: String, val clusterName: String) {
    private val resources = Resources()
    private val snapshot = Resources()

    fun listClusterNames(): List<String> = snapshot.flinkClusters.keys.toList()

    fun getFlinkClusters(): List<V1FlinkCluster> = snapshot.flinkClusters.values.toList()

    fun getFlinkCluster(clusterName: String) : V1FlinkCluster? = snapshot.flinkClusters[clusterName]

    fun getClusterResources() =
        ClusterResources(
            flinkCluster = snapshot.flinkClusters[clusterName],
            flinkJobs = snapshot.flinkJobs.values.toSet(),
            jobmanagerPods = snapshot.jobmanagerPods.values.toSet(),
            taskmanagerPods = snapshot.taskmanagerPods.values.toSet(),
            jobmanagerService = snapshot.services.values.firstOrNull()
        )

    fun getJobResources(jobName: String) =
        JobResources(
            flinkJob = snapshot.flinkJobs["$clusterName-$jobName"],
            bootstrapJob = snapshot.bootstrapJobs["bootstrap-$clusterName-$jobName"]
        )

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        if (extractNamespace(resource) == namespace && extractName(resource) == clusterName) {
            resources.flinkClusters[extractName(resource)] = resource
        }
    }

    fun onFlinkClusterDeleted(resource: V1FlinkCluster) {
        if (extractNamespace(resource) == namespace) {
            resources.flinkClusters.remove(extractName(resource))
        }
    }

    fun onFlinkClusterDeletedAll() {
        resources.flinkClusters.clear()
    }

    fun onFlinkJobChanged(resource: V1FlinkJob) {
        if (extractNamespace(resource) == namespace && extractName(resource).startsWith("${clusterName}-")) {
            resources.flinkJobs[extractName(resource)] = resource
        }
    }

    fun onFlinkJobDeleted(resource: V1FlinkJob) {
        if (extractNamespace(resource) == namespace) {
            resources.flinkJobs.remove(extractName(resource))
        }
    }

    fun onFlinkJobDeletedAll() {
        resources.flinkJobs.clear()
    }

    fun onServiceChanged(resource: V1Service) {
        if (extractNamespace(resource) == namespace && extractName(resource) == "jobmanager-${clusterName}") {
            resources.services[extractName(resource)] = resource
        }
    }

    fun onServiceDeleted(resource: V1Service) {
        if (extractNamespace(resource) == namespace) {
            resources.services.remove(extractName(resource))
        }
    }

    fun onServiceDeletedAll() {
        resources.services.clear()
    }

    fun onJobChanged(resource: V1Job) {
        if (extractNamespace(resource) == namespace && extractName(resource).startsWith("bootstrap-${clusterName}-")) {
            resources.bootstrapJobs[extractName(resource)] = resource
        }
    }

    fun onJobDeleted(resource: V1Job) {
        if (extractNamespace(resource) == namespace) {
            resources.bootstrapJobs.remove(extractName(resource))
        }
    }

    fun onJobDeletedAll() {
        resources.bootstrapJobs.clear()
    }

    fun onPodChanged(resource: V1Pod) {
        if (extractNamespace(resource) == namespace && extractName(resource).startsWith("jobmanager-${clusterName}-")) {
            resources.jobmanagerPods[extractName(resource)] = resource
        }
        if (extractNamespace(resource) == namespace && extractName(resource).startsWith("taskmanager-${clusterName}-")) {
            resources.taskmanagerPods[extractName(resource)] = resource
        }
    }

    fun onPodDeleted(resource: V1Pod) {
        if (extractNamespace(resource) == namespace) {
            resources.jobmanagerPods.remove(extractName(resource))
            resources.taskmanagerPods.remove(extractName(resource))
        }
    }

    fun onPodDeletedAll() {
        resources.jobmanagerPods.clear()
        resources.taskmanagerPods.clear()
    }

    fun updateSnapshot() {
        snapshot.flinkClusters = ConcurrentHashMap(resources.flinkClusters)
        snapshot.bootstrapJobs = ConcurrentHashMap(resources.bootstrapJobs)
        snapshot.jobmanagerPods = ConcurrentHashMap(resources.jobmanagerPods)
        snapshot.taskmanagerPods = ConcurrentHashMap(resources.taskmanagerPods)
        snapshot.flinkJobs = ConcurrentHashMap(resources.flinkJobs)
        snapshot.services = ConcurrentHashMap(resources.services)
    }

    private fun extractName(resource: V1Pod) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractName(resource: V1Job) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractName(resource: V1Service) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractName(resource: V1FlinkJob) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractName(resource: V1FlinkCluster) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractNamespace(resource: V1Pod) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private fun extractNamespace(resource: V1Job) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private fun extractNamespace(resource: V1Service) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private fun extractNamespace(resource: V1FlinkJob) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private fun extractNamespace(resource: V1FlinkCluster) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private class Resources {
        @Volatile
        var flinkClusters = ConcurrentHashMap<String, V1FlinkCluster>()
        @Volatile
        var bootstrapJobs = ConcurrentHashMap<String, V1Job>()
        @Volatile
        var jobmanagerPods = ConcurrentHashMap<String, V1Pod>()
        @Volatile
        var taskmanagerPods = ConcurrentHashMap<String, V1Pod>()
        @Volatile
        var flinkJobs = ConcurrentHashMap<String, V1FlinkJob>()
        @Volatile
        var services = ConcurrentHashMap<String, V1Service>()
    }
}