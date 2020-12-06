package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod
import java.util.concurrent.ConcurrentHashMap

class Cache(val namespace: String) {
    private val resources = Resources()
    private val snapshot = Resources()

    fun listDeploymentNames(): List<String> = snapshot.flinkDeployments.keys.toList()

    fun listClusterNames(): List<String> = snapshot.flinkClusters.keys.toList()

    fun listJobNames(): List<String> = snapshot.flinkJobs.keys.toList()

    fun getFlinkDeployments(): List<V1FlinkDeployment> = snapshot.flinkDeployments.values.toList()

    fun getFlinkDeployment(deploymentName: String) = snapshot.flinkDeployments[deploymentName]

    fun getFlinkClusters(): List<V1FlinkCluster> = snapshot.flinkClusters.values.toList()

    fun getFlinkCluster(clusterName: String) = snapshot.flinkClusters[clusterName]

    fun getFlinkJobs(): List<V1FlinkJob> = snapshot.flinkJobs.values.toList()

    fun getFlinkJob(jobName: String) = snapshot.flinkJobs[jobName]

    fun getSupervisorResources(clusterName: String) =
        SupervisorResources(
            flinkCluster = snapshot.flinkClusters[clusterName],
            supervisorPod = snapshot.supervisorPods.values.filter { it.metadata?.name?.startsWith("supervisor-$clusterName-") ?: false }.firstOrNull(),
            supervisorDep = snapshot.supervisorDeps["supervisor-$clusterName"]
        )

    fun onFlinkDeploymentChanged(resource: V1FlinkDeployment) {
        if (extractNamespace(resource) == namespace) {
            resources.flinkDeployments[extractName(resource)] = resource
        }
    }

    fun onFlinkDeploymentDeleted(resource: V1FlinkDeployment) {
        if (extractNamespace(resource) == namespace) {
            resources.flinkDeployments.remove(extractName(resource))
        }
    }

    fun onFlinkDeploymentDeletedAll() {
        resources.flinkDeployments.clear()
    }

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        if (extractNamespace(resource) == namespace) {
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
        if (extractNamespace(resource) == namespace) {
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

    fun onPodChanged(resource: V1Pod) {
        if (extractNamespace(resource) == namespace && extractName(resource).startsWith("supervisor-")) {
            resources.supervisorPods[extractName(resource)] = resource
        }
    }

    fun onPodDeleted(resource: V1Pod) {
        if (extractNamespace(resource) == namespace) {
            resources.supervisorPods.remove(extractName(resource))
        }
    }

    fun onPodDeletedAll() {
        resources.supervisorPods.clear()
    }

    fun onDeploymentChanged(resource: V1Deployment) {
        if (extractNamespace(resource) == namespace && extractName(resource).startsWith("supervisor-")) {
            resources.supervisorDeps[extractName(resource)] = resource
        }
    }

    fun onDeploymentDeleted(resource: V1Deployment) {
        if (extractNamespace(resource) == namespace) {
            resources.supervisorDeps.remove(extractName(resource))
        }
    }

    fun onDeploymentDeletedAll() {
        resources.supervisorDeps.clear()
    }

    fun updateSnapshot() {
        snapshot.flinkDeployments = ConcurrentHashMap(resources.flinkDeployments)
        snapshot.flinkClusters = ConcurrentHashMap(resources.flinkClusters)
        snapshot.flinkJobs = ConcurrentHashMap(resources.flinkJobs)
        snapshot.supervisorPods = ConcurrentHashMap(resources.supervisorPods)
        snapshot.supervisorDeps = ConcurrentHashMap(resources.supervisorDeps)
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

    private class Resources {
        @Volatile
        var flinkDeployments = ConcurrentHashMap<String, V1FlinkDeployment>()
        @Volatile
        var flinkClusters = ConcurrentHashMap<String, V1FlinkCluster>()
        @Volatile
        var flinkJobs = ConcurrentHashMap<String, V1FlinkJob>()
        @Volatile
        var supervisorPods = ConcurrentHashMap<String, V1Pod>()
        @Volatile
        var supervisorDeps = ConcurrentHashMap<String, V1Deployment>()
    }
}