package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod

class Cache(val namespace: String) {
    private val resources = Resources()
    private val snapshot = Snapshot()
    private var latestResetTimestamp: Long = 0L
    private var latestUpdateTimestamp: Long = 0L

    fun listDeploymentNames(): List<String> = snapshot.flinkDeployments.keys.toList()

    fun listClusterNames(): List<String> = snapshot.flinkClusters.keys.toList()

    fun listJobNames(): List<String> = snapshot.flinkJobs.keys.toList()

    fun getFlinkDeployments(): List<V1FlinkDeployment> = snapshot.flinkDeployments.values.map { it.resource }.toList()

    fun getFlinkDeployment(deploymentName: String) = snapshot.flinkDeployments[deploymentName]?.resource

    fun getFlinkClusters(): List<V1FlinkCluster> = snapshot.flinkClusters.values.map { it.resource }.toList()

    fun getFlinkCluster(clusterName: String) = snapshot.flinkClusters[clusterName]?.resource

    fun getFlinkJobs(): List<V1FlinkJob> = snapshot.flinkJobs.values.map { it.resource }.toList()

    fun getFlinkJob(jobName: String) = snapshot.flinkJobs[jobName]?.resource

    fun getSupervisorResources(clusterName: String) =
        SupervisorResources(
            flinkCluster = snapshot.flinkClusters[clusterName]?.resource,
            supervisorPods = snapshot.supervisorPods.values.map { it.resource }.filter { it.metadata?.name?.startsWith("supervisor-$clusterName-") ?: false }.toSet(),
            supervisorDep = snapshot.supervisorDeps["supervisor-$clusterName"]?.resource
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

    fun onFlinkDeploymentChanged(resource: V1FlinkDeployment) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.flinkDeployments[extractName(resource)] = CachedResource(resource, latestUpdateTimestamp)
            }
        }
    }

    fun onFlinkDeploymentDeleted(resource: V1FlinkDeployment) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.flinkDeployments.remove(extractName(resource))
            }
        }
    }

    fun onFlinkDeploymentsReset() {
        synchronized(resources) {
            latestResetTimestamp = System.currentTimeMillis()
            resources.flinkDeployments.clear()
        }
    }

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
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
            if (extractNamespace(resource) == namespace) {
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

    fun onPodChanged(resource: V1Pod) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace && extractName(resource).startsWith("supervisor-")) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.supervisorPods[extractName(resource)] = CachedResource(resource, latestUpdateTimestamp)
            }
        }
    }

    fun onPodDeleted(resource: V1Pod) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.supervisorPods.remove(extractName(resource))
            }
        }
    }

    fun onPodsReset() {
        synchronized(resources) {
            latestResetTimestamp = System.currentTimeMillis()
            resources.supervisorPods.clear()
        }
    }

    fun onDeploymentChanged(resource: V1Deployment) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace && extractName(resource).startsWith("supervisor-")) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.supervisorDeps[extractName(resource)] = CachedResource(resource, latestUpdateTimestamp)
            }
        }
    }

    fun onDeploymentDeleted(resource: V1Deployment) {
        synchronized(resources) {
            if (extractNamespace(resource) == namespace) {
                latestUpdateTimestamp = System.currentTimeMillis()
                resources.supervisorDeps.remove(extractName(resource))
            }
        }
    }

    fun onDeploymentsReset() {
        synchronized(resources) {
            latestResetTimestamp = System.currentTimeMillis()
            resources.supervisorDeps.clear()
        }
    }

    fun takeSnapshot() {
        synchronized(resources) {
            snapshot.flinkDeployments = resources.flinkDeployments.toMap()
            snapshot.flinkClusters = resources.flinkClusters.toMap()
            snapshot.flinkJobs = resources.flinkJobs.toMap()
            snapshot.supervisorPods = resources.supervisorPods.toMap()
            snapshot.supervisorDeps = resources.supervisorDeps.toMap()
        }
    }

    private fun extractName(resource: V1Pod) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractName(resource: V1Deployment) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractName(resource: V1FlinkJob) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractName(resource: V1FlinkCluster) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractName(resource: V1FlinkDeployment) =
        resource.metadata?.name ?: throw RuntimeException("Name is null")

    private fun extractNamespace(resource: V1Pod) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private fun extractNamespace(resource: V1Deployment) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private fun extractNamespace(resource: V1FlinkJob) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private fun extractNamespace(resource: V1FlinkCluster) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private fun extractNamespace(resource: V1FlinkDeployment) =
        resource.metadata?.namespace ?: throw RuntimeException("Namespace is null")

    private data class CachedResource<T>(val resource: T, val timestamp: Long)

    private class Resources {
        var flinkDeployments = mutableMapOf<String, CachedResource<V1FlinkDeployment>>()
        var flinkClusters = mutableMapOf<String, CachedResource<V1FlinkCluster>>()
        var flinkJobs = mutableMapOf<String, CachedResource<V1FlinkJob>>()
        var supervisorPods = mutableMapOf<String, CachedResource<V1Pod>>()
        var supervisorDeps = mutableMapOf<String, CachedResource<V1Deployment>>()
    }

    private class Snapshot {
        var flinkDeployments = mapOf<String, CachedResource<V1FlinkDeployment>>()
        var flinkClusters = mapOf<String, CachedResource<V1FlinkCluster>>()
        var flinkJobs = mapOf<String, CachedResource<V1FlinkJob>>()
        var supervisorPods = mapOf<String, CachedResource<V1Pod>>()
        var supervisorDeps = mapOf<String, CachedResource<V1Deployment>>()
    }
}