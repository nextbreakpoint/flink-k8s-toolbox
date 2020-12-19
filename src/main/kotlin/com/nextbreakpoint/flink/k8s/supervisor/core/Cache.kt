package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service

class Cache(val namespace: String, val clusterName: String) {
    private val resources = Resources()
    private val snapshot = Snapshot()
    private var latestResetTimestamp: Long = 0L
    private var latestUpdateTimestamp: Long = 0L

    fun listClusterNames(): List<String> = snapshot.flinkClusters.keys.toList()

    fun getFlinkClusters(): List<V1FlinkCluster> = snapshot.flinkClusters.values.map { it.resource }.toList()

    fun getFlinkCluster(clusterName: String) : V1FlinkCluster? = snapshot.flinkClusters[clusterName]?.resource

    fun getClusterResources() =
        ClusterResources(
            flinkCluster = snapshot.flinkClusters[clusterName]?.resource,
            flinkJobs = snapshot.flinkJobs.values.map { it.resource }.toSet(),
            jobmanagerPods = snapshot.jobmanagerPods.values.map { it.resource }.toSet(),
            taskmanagerPods = snapshot.taskmanagerPods.values.map { it.resource }.toSet(),
            jobmanagerService = snapshot.services.values.map { it.resource }.firstOrNull()
        )

    fun getJobResources(jobName: String) =
        JobResources(
            flinkJob = snapshot.flinkJobs["$clusterName-$jobName"]?.resource,
            bootstrapJob = snapshot.bootstrapJobs["bootstrap-$clusterName-$jobName"]?.resource
        )

    fun getLastResetTimestamp(): Long {
        synchronized(resources) {
            return latestResetTimestamp
        }
    }

    fun getLastUpdateTimestamp(): Long {
        synchronized(resources) {
            return latestUpdateTimestamp
        }
    }

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace && extractName(resource) == clusterName) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.flinkClusters[extractName(resource)] = CachedResource(resource, latestUpdateTimestamp)
            }
        }
    }

    fun onFlinkClusterDeleted(resource: V1FlinkCluster) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.flinkClusters.remove(extractName(resource))
            }
        }
    }

    fun onFlinkClustersReset() {
        synchronized(resources) {
            latestResetTimestamp = System.currentTimeMillis()
            resources.flinkClusters.clear()
        }
    }

    fun onFlinkJobChanged(resource: V1FlinkJob) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace && extractName(resource).startsWith("${clusterName}-")) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.flinkJobs[extractName(resource)] = CachedResource(resource, latestUpdateTimestamp)
            }
        }
    }

    fun onFlinkJobDeleted(resource: V1FlinkJob) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.flinkJobs.remove(extractName(resource))
            }
        }
    }

    fun onFlinkJobsReset() {
        synchronized(resources) {
            latestResetTimestamp = System.currentTimeMillis()
            resources.flinkJobs.clear()
        }
    }

    fun onServiceChanged(resource: V1Service) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace && extractName(resource) == "jobmanager-${clusterName}") {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.services[extractName(resource)] = CachedResource(resource, latestUpdateTimestamp)
            }
        }
    }

    fun onServiceDeleted(resource: V1Service) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.services.remove(extractName(resource))
            }
        }
    }

    fun onServicesReset() {
        synchronized(resources) {
            latestResetTimestamp = System.currentTimeMillis()
            resources.services.clear()
        }
    }

    fun onJobChanged(resource: V1Job) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace && extractName(resource).startsWith("bootstrap-${clusterName}-")) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.bootstrapJobs[extractName(resource)] = CachedResource(resource, latestUpdateTimestamp)
            }
        }
    }

    fun onJobDeleted(resource: V1Job) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.bootstrapJobs.remove(extractName(resource))
            }
        }
    }

    fun onJobsReset() {
        synchronized(resources) {
            latestResetTimestamp = System.currentTimeMillis()
            resources.bootstrapJobs.clear()
        }
    }

    fun onPodChanged(resource: V1Pod) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace && extractName(resource).startsWith("jobmanager-${clusterName}-")) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.jobmanagerPods[extractName(resource)] = CachedResource(resource, latestUpdateTimestamp)
            }
            if (extractNamespace(resource) == namespace && extractName(resource).startsWith("taskmanager-${clusterName}-")) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.taskmanagerPods[extractName(resource)] = CachedResource(resource, latestUpdateTimestamp)
            }
        }
    }

    fun onPodDeleted(resource: V1Pod) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.jobmanagerPods.remove(extractName(resource))
                resources.taskmanagerPods.remove(extractName(resource))
            }
        }
    }

    fun onPodsReset() {
        synchronized(resources) {
            latestResetTimestamp = System.currentTimeMillis()
            resources.jobmanagerPods.clear()
            resources.taskmanagerPods.clear()
        }
    }

    fun takeSnapshot() {
        synchronized(resources) {
            snapshot.flinkClusters = resources.flinkClusters.toMap()
            snapshot.flinkJobs = resources.flinkJobs.toMap()
            snapshot.bootstrapJobs = resources.bootstrapJobs.toMap()
            snapshot.jobmanagerPods = resources.jobmanagerPods.toMap()
            snapshot.taskmanagerPods = resources.taskmanagerPods.toMap()
            snapshot.services = resources.services.toMap()
        }
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

    private data class CachedResource<T>(val resource: T, val timestamp: Long)

    private class Resources {
        var flinkClusters = mutableMapOf<String, CachedResource<V1FlinkCluster>>()
        var flinkJobs = mutableMapOf<String, CachedResource<V1FlinkJob>>()
        var bootstrapJobs = mutableMapOf<String, CachedResource<V1Job>>()
        var jobmanagerPods = mutableMapOf<String, CachedResource<V1Pod>>()
        var taskmanagerPods = mutableMapOf<String, CachedResource<V1Pod>>()
        var services = mutableMapOf<String, CachedResource<V1Service>>()
    }

    private class Snapshot {
        var flinkClusters = mapOf<String, CachedResource<V1FlinkCluster>>()
        var flinkJobs = mapOf<String, CachedResource<V1FlinkJob>>()
        var bootstrapJobs = mapOf<String, CachedResource<V1Job>>()
        var jobmanagerPods = mapOf<String, CachedResource<V1Pod>>()
        var taskmanagerPods = mapOf<String, CachedResource<V1Pod>>()
        var services = mapOf<String, CachedResource<V1Service>>()
    }
}