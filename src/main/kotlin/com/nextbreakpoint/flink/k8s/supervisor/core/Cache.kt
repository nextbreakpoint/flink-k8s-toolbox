package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.crd.V2FlinkCluster
import com.nextbreakpoint.flink.common.ResourceSelector
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1ObjectMeta
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service
import java.util.concurrent.ConcurrentHashMap

class Cache(val namespace: String, val clusterName: String) {
    private val resources = Resources()
    private val snapshot = Resources()

    fun getClusterSelectors(): List<ResourceSelector> = snapshot.flinkClusters.keys.toList()

    fun findClusterSelector(namespace: String, name: String) : ResourceSelector? =
        snapshot.flinkClusters.keys.firstOrNull {
            it.namespace == namespace && it.name == name
        }

    fun getFlinkClusters(): List<V2FlinkCluster> = snapshot.flinkClusters.values.toList()

    fun getFlinkCluster(clusterSelector: ResourceSelector) = snapshot.flinkClusters[clusterSelector]
        ?: throw RuntimeException("Cluster not found ${clusterSelector.name}")

    fun getCachedClusterResources(clusterSelector: ResourceSelector) =
        ClusterResources(
            flinkCluster = snapshot.flinkClusters[clusterSelector],
            flinkJobs = snapshot.flinkJobs[clusterSelector].orEmpty(),
            jobmanagerPods = snapshot.jobmanagerPods[clusterSelector]?.values.orEmpty().toSet(),
            taskmanagerPods = snapshot.taskmanagerPods[clusterSelector]?.values.orEmpty().toSet(),
            service = snapshot.services[clusterSelector]
        )

    fun getCachedJobResources(clusterSelector: ResourceSelector, jobName: String) =
        JobResources(
            flinkJob = snapshot.flinkJobs[clusterSelector]?.get(jobName),
            bootstrapJob = snapshot.bootstrapJobs[clusterSelector]?.get(jobName)
        )

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

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        resources.flinkClusters[clusterSelector] = resource
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

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        resources.flinkClusters.remove(clusterSelector)
    }

    fun onFlinkClusterDeletedAll() {
        resources.flinkClusters.clear()
    }

    fun onServiceChanged(resource: V1Service) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val clusterSelector = makeClusterSelector(metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        resources.services[clusterSelector] = resource
    }

    fun onServiceDeleted(resource: V1Service) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val clusterSelector = makeClusterSelector(metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        resources.services.remove(clusterSelector)
    }

    fun onServiceDeletedAll() {
        resources.services.clear()
    }

    fun onJobChanged(resource: V1Job) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val jobName = extractJobName(metadata)

        val clusterSelector = makeClusterSelector(metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        val jobs = resources.bootstrapJobs[clusterSelector] ?: mutableMapOf()
        jobs[jobName] = resource
        resources.bootstrapJobs[clusterSelector] = jobs
    }

    fun onJobDeleted(resource: V1Job) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val jobName = extractJobName(metadata)

        val clusterSelector = makeClusterSelector(metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        val jobs = resources.bootstrapJobs[clusterSelector]
        if (jobs != null) {
            jobs.remove(jobName)
            if (jobs.isEmpty()) {
                resources.bootstrapJobs.remove(clusterSelector)
            }
        }
    }

    fun onJobDeletedAll() {
        resources.bootstrapJobs.clear()
    }

    fun onPodChanged(resource: V1Pod) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val name = metadata.name ?: throw RuntimeException("Missing name")

        val clusterSelector = makeClusterSelector(metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        when {
            metadata.labels?.get("role") == "jobmanager" -> {
                val pods = resources.jobmanagerPods[clusterSelector] ?: mutableMapOf()
                pods[name] = resource
                resources.jobmanagerPods[clusterSelector] = pods
            }

            metadata.labels?.get("role") == "taskmanager" -> {
                val pods = resources.taskmanagerPods[clusterSelector] ?: mutableMapOf()
                pods[name] = resource
                resources.taskmanagerPods[clusterSelector] = pods
            }
        }
    }

    fun onPodDeleted(resource: V1Pod) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val name = metadata.name ?: throw RuntimeException("Missing name")

        val clusterSelector = makeClusterSelector(metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        when {
            metadata.labels?.get("role") == "jobmanager" -> {
                val pods = resources.jobmanagerPods[clusterSelector]
                if (pods != null) {
                    pods.remove(name)
                    if (pods.isEmpty()) {
                        resources.jobmanagerPods.remove(clusterSelector)
                    }
                }
            }

            metadata.labels?.get("role") == "taskmanager" ->  {
                val pods = resources.taskmanagerPods[clusterSelector]
                if (pods != null) {
                    pods.remove(name)
                    if (pods.isEmpty()) {
                        resources.taskmanagerPods.remove(clusterSelector)
                    }
                }
            }
        }
    }

    fun onPodDeletedAll() {
        resources.jobmanagerPods.clear()
        resources.taskmanagerPods.clear()
    }

    fun onFlinkJobChanged(resource: V1FlinkJob) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val jobName = extractJobName(metadata)

        val clusterSelector = makeClusterSelector(metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        val jobs = resources.flinkJobs[clusterSelector] ?: mutableMapOf()
        jobs[jobName] = resource
        resources.flinkJobs[clusterSelector] = jobs
    }

    fun onFlinkJobDeleted(resource: V1FlinkJob) {
        val metadata = resource.metadata ?: throw RuntimeException("Missing metadata")

        val jobName = extractJobName(metadata)

        val clusterSelector = makeClusterSelector(metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        val jobs = resources.flinkJobs[clusterSelector]
        if (jobs != null) {
            jobs.remove(jobName)
            if (jobs.isEmpty()) {
                resources.flinkJobs.remove(clusterSelector)
            }
        }
    }

    fun onFlinkJobsDeletedAll() {
        resources.flinkJobs.clear()
    }

    fun updateSnapshot() {
        snapshot.flinkClusters.clear()
        snapshot.bootstrapJobs.clear()
        snapshot.jobmanagerPods.clear()
        snapshot.taskmanagerPods.clear()
        snapshot.flinkJobs.clear()
        snapshot.services.clear()
        snapshot.flinkClusters.putAll(resources.flinkClusters)
        snapshot.bootstrapJobs.putAll(resources.bootstrapJobs)
        snapshot.jobmanagerPods.putAll(resources.jobmanagerPods)
        snapshot.taskmanagerPods.putAll(resources.taskmanagerPods)
        snapshot.flinkJobs.putAll(resources.flinkJobs)
        snapshot.services.putAll(resources.services)
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

    private fun extractJobName(objectMeta: V1ObjectMeta) =
        objectMeta.labels?.get("jobName") ?: throw RuntimeException("Missing required label jobName")

    private fun acceptClusterSelector(clusterSelector: ResourceSelector) =
        clusterSelector.namespace == namespace && clusterSelector.name == clusterName

    private class Resources {
        val flinkClusters = ConcurrentHashMap<ResourceSelector, V2FlinkCluster>()
        val bootstrapJobs = ConcurrentHashMap<ResourceSelector, MutableMap<String, V1Job>>()
        val jobmanagerPods = ConcurrentHashMap<ResourceSelector, MutableMap<String, V1Pod>>()
        val taskmanagerPods = ConcurrentHashMap<ResourceSelector, MutableMap<String, V1Pod>>()
        val flinkJobs = ConcurrentHashMap<ResourceSelector, MutableMap<String, V1FlinkJob>>()
        val services = ConcurrentHashMap<ResourceSelector, V1Service>()
    }
}