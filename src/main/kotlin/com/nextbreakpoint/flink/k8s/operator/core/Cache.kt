package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.k8s.supervisor.core.Cache
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1ObjectMeta
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service
import java.util.concurrent.ConcurrentHashMap

class Cache(val namespace: String) {
    private val resources = Resources()
    private val snapshot = Resources()

    fun getClusterSelectorsV1(): List<ResourceSelector> = snapshot.flinkClustersV1.keys.toList()

    fun getClusterSelectorsV2(): List<ResourceSelector> = snapshot.flinkClustersV2.keys.toList()

    fun findClusterSelectorV1(namespace: String, clusterName: String) =
        snapshot.flinkClustersV1.keys.firstOrNull {
            it.namespace == namespace && it.name == clusterName
        } ?: throw RuntimeException("Cluster selector not found")

    fun findClusterSelectorV2(namespace: String, clusterName: String) =
        snapshot.flinkClustersV2.keys.firstOrNull {
            it.namespace == namespace && it.name == clusterName
        } ?: throw RuntimeException("Cluster selector not found")

    fun findJobSelector(namespace: String, jobName: String) =
        snapshot.flinkJobs.keys.firstOrNull {
            it.namespace == namespace && it.name == jobName
        } ?: throw RuntimeException("Job selector not found")

    fun findJobId(jobSelector: ResourceSelector) =
        getFlinkJob(jobSelector).status.jobId ?: throw RuntimeException("Job id missing")

    fun getFlinkClustersV1(): List<V1FlinkCluster> = snapshot.flinkClustersV1.values.toList()

    fun getFlinkClustersV2(): List<V2FlinkCluster> = snapshot.flinkClustersV2.values.toList()

    fun getFlinkClusterV1(clusterSelector: ResourceSelector) = snapshot.flinkClustersV1[clusterSelector]
            ?: throw RuntimeException("Cluster resource not found ${clusterSelector.name}")

    fun getFlinkClusterV2(clusterSelector: ResourceSelector) = snapshot.flinkClustersV2[clusterSelector]
        ?: throw RuntimeException("Cluster resource not found ${clusterSelector.name}")

    fun getFlinkJob(jobSelector: ResourceSelector) = snapshot.flinkJobs[jobSelector]
        ?: throw RuntimeException("Job resource not found ${jobSelector.name}")

    fun getCachedResources(clusterSelector: ResourceSelector) =
        CachedResources(
            flinkCluster = snapshot.flinkClustersV2[clusterSelector],
            supervisorPod = snapshot.supervisorPods[clusterSelector],
            supervisorDeployment = snapshot.supervisorDeployments[clusterSelector]
        )

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val clusterSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.flinkClustersV1[clusterSelector] = resource
    }

    fun onFlinkClusterDeleted(resource: V1FlinkCluster) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val clusterSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.flinkClustersV1.remove(clusterSelector)
    }

    fun onFlinkClusterDeletedAllV1() {
        resources.flinkClustersV1.clear()
    }

    fun onFlinkClusterChanged(resource: V2FlinkCluster) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val clusterSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.flinkClustersV2[clusterSelector] = resource
    }

    fun onFlinkClusterDeleted(resource: V2FlinkCluster) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val clusterSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.flinkClustersV2.remove(clusterSelector)
    }

    fun onFlinkClusterDeletedAllV2() {
        resources.flinkClustersV2.clear()
    }

    fun onServiceChanged(resource: V1Service) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val serviceSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.services[serviceSelector] = resource
    }

    fun onServiceDeleted(resource: V1Service) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val serviceSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.services.remove(serviceSelector)
    }

    fun onServiceDeletedAll() {
        resources.services.clear()
    }

    fun onJobChanged(resource: V1Job) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val bootstrapSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.bootstrapJobs[bootstrapSelector] = resource
    }

    fun onJobDeleted(resource: V1Job) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val bootstrapSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.bootstrapJobs.remove(bootstrapSelector)
    }

    fun onJobDeletedAll() {
        resources.bootstrapJobs.clear()
    }

    fun onPodChanged(resource: V1Pod) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val podSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.pods[podSelector] = resource

        val clusterSelector = makeClusterSelector(metadata)

        when {
            metadata.labels?.get("role") == "supervisor" -> {
                resources.supervisorPods[clusterSelector] = resource
            }
        }
    }

    fun onPodDeleted(resource: V1Pod) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val podSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.pods.remove(podSelector)

        val clusterSelector = makeClusterSelector(metadata)

        when {
            metadata.labels?.get("role") == "supervisor" -> {
                resources.supervisorPods.remove(clusterSelector)
            }
        }
    }

    fun onPodDeletedAll() {
        resources.pods.clear()
        resources.supervisorPods.clear()
    }

    fun onDeploymentChanged(resource: V1Deployment) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val clusterSelector = makeClusterSelector(metadata)

        when {
            metadata.labels?.get("role") == "supervisor" -> {
                resources.supervisorDeployments[clusterSelector] = resource
            }
        }
    }

    fun onDeploymentDeleted(resource: V1Deployment) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val clusterSelector = makeClusterSelector(metadata)

        when {
            metadata.labels?.get("role") == "supervisor" -> {
                resources.supervisorDeployments.remove(clusterSelector)
            }
        }
    }

    fun onDeploymentDeletedAll() {
        resources.supervisorDeployments.clear()
    }

    fun onFlinkJobChanged(resource: V1FlinkJob) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val jobSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.flinkJobs[jobSelector] = resource
    }

    fun onFlinkJobDeleted(resource: V1FlinkJob) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")
        val namespace = metadata.namespace ?: throw RuntimeException("Missing namespace")
        val name = metadata.name ?: throw RuntimeException("Missing name")
        val uid = metadata.uid ?: throw RuntimeException("Missing uid")

        val jobSelector = ResourceSelector(
            namespace = namespace,
            name = name,
            uid = uid
        )

        resources.flinkJobs.remove(jobSelector)
    }

    fun onFlinkJobsDeletedAll() {
        resources.flinkJobs.clear()
    }

    fun updateSnapshot() {
        snapshot.flinkClustersV1.clear()
        snapshot.flinkClustersV2.clear()
        snapshot.flinkJobs.clear()
        snapshot.supervisorPods.clear()
        snapshot.supervisorDeployments.clear()
        snapshot.bootstrapJobs.clear()
        snapshot.services.clear()
        snapshot.bootstrapJobs.clear()
        snapshot.flinkClustersV1.putAll(resources.flinkClustersV1)
        snapshot.flinkClustersV2.putAll(resources.flinkClustersV2)
        snapshot.flinkJobs.putAll(resources.flinkJobs)
        snapshot.supervisorPods.putAll(resources.supervisorPods)
        snapshot.supervisorDeployments.putAll(resources.supervisorDeployments)
        snapshot.bootstrapJobs.putAll(resources.bootstrapJobs)
        snapshot.services.putAll(resources.services)
        snapshot.pods.putAll(resources.pods)
    }

    private fun makeClusterSelector(metadata: V1ObjectMeta) =
        ResourceSelector(
            namespace = extractNamespace(metadata),
            name = extractClusterName(metadata),
            uid = extractClusterUid(metadata)
        )

    private fun extractNamespace(objectMeta: V1ObjectMeta) =
        objectMeta.namespace ?: throw RuntimeException("Missing namespace")

    private fun extractClusterName(objectMeta: V1ObjectMeta) =
        objectMeta.labels?.get("clusterName") ?: throw RuntimeException("Missing required label clusterName")

    private fun extractClusterUid(objectMeta: V1ObjectMeta) =
        objectMeta.labels?.get("clusterUid") ?: throw RuntimeException("Missing required label clusterUid")

    private class Resources {
        val flinkClustersV1 = ConcurrentHashMap<ResourceSelector, V1FlinkCluster>()
        val flinkClustersV2 = ConcurrentHashMap<ResourceSelector, V2FlinkCluster>()
        val flinkJobs = ConcurrentHashMap<ResourceSelector, V1FlinkJob>()
        val supervisorPods = ConcurrentHashMap<ResourceSelector, V1Pod>()
        val supervisorDeployments = ConcurrentHashMap<ResourceSelector, V1Deployment>()
        val bootstrapJobs = ConcurrentHashMap<ResourceSelector, V1Job>()
        val services = ConcurrentHashMap<ResourceSelector, V1Service>()
        val pods = ConcurrentHashMap<ResourceSelector, V1Pod>()
    }
}