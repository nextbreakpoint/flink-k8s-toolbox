package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1Pod
import io.kubernetes.client.models.V1Service
import java.util.concurrent.ConcurrentHashMap

class SupervisorCache(val namespace: String, val clusterName: String) {
    private val flinkClusters = ConcurrentHashMap<ClusterSelector, V1FlinkCluster>()
    private val bootstrapJobs = ConcurrentHashMap<ClusterSelector, V1Job>()
    private val jobmanagerPods = ConcurrentHashMap<ClusterSelector, MutableMap<String, V1Pod>>()
    private val taskmanagerPods = ConcurrentHashMap<ClusterSelector, MutableMap<String, V1Pod>>()
    private val services = ConcurrentHashMap<ClusterSelector, V1Service>()

    fun getClusterSelectors(): List<ClusterSelector> = flinkClusters.keys.toList()

    fun findClusterSelector(namespace: String, name: String) =
            flinkClusters.keys.firstOrNull {
                it.namespace == namespace && it.name == name
            }

    fun getFlinkClusters(): List<V1FlinkCluster> = flinkClusters.values.toList()

    fun getFlinkCluster(clusterSelector: ClusterSelector) = flinkClusters[clusterSelector]
            ?: throw RuntimeException("Cluster not found ${clusterSelector.name}")

    fun getCachedResources(clusterSelector: ClusterSelector) =
            SupervisorCachedResources(
                flinkCluster = flinkClusters[clusterSelector],
                bootstrapJob = bootstrapJobs[clusterSelector],
                jobmanagerPods = jobmanagerPods[clusterSelector]?.values?.toSet() ?: setOf(),
                taskmanagerPods = taskmanagerPods[clusterSelector]?.values?.toSet() ?: setOf(),
                service = services[clusterSelector]
            )

    fun onFlinkClusterChanged(resource: V1FlinkCluster) {
        val clusterSelector = ClusterSelector(
                namespace = resource.metadata.namespace,
                name = resource.metadata.name,
                uuid = resource.metadata.uid
        )

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        flinkClusters[clusterSelector] = resource
    }

    fun onFlinkClusterDeleted(resource: V1FlinkCluster) {
        val clusterSelector = ClusterSelector(
                namespace = resource.metadata.namespace,
                name = resource.metadata.name,
                uuid = resource.metadata.uid
        )

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        flinkClusters.remove(clusterSelector)
    }

    fun onFlinkClusterDeletedAll() {
        flinkClusters.clear()
    }

    fun onServiceChanged(resource: V1Service) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        services[clusterSelector] = resource
    }

    fun onServiceDeleted(resource: V1Service) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        services.remove(clusterSelector)
    }

    fun onJobChanged(resource: V1Job) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        bootstrapJobs[clusterSelector] = resource
    }

    fun onJobDeleted(resource: V1Job) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        bootstrapJobs.remove(clusterSelector)
    }

    fun onPodChanged(resource: V1Pod) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        when {
            resource.metadata.labels.get("role") == "jobmanager" -> {
                val pods = jobmanagerPods[clusterSelector] ?: mutableMapOf()
                pods.put(resource.metadata.name, resource)
                jobmanagerPods[clusterSelector] = pods
            }

            resource.metadata.labels.get("role") == "taskmanager" -> {
                val pods = taskmanagerPods[clusterSelector] ?: mutableMapOf()
                pods.put(resource.metadata.name, resource)
                taskmanagerPods[clusterSelector] = pods
            }
        }
    }

    fun onPodDeleted(resource: V1Pod) {
        val clusterSelector = makeClusterSelector(resource.metadata)

        if (!acceptClusterSelector(clusterSelector)) {
            return
        }

        when {
            resource.metadata.labels.get("role") == "jobmanager" -> {
                val pods = jobmanagerPods[clusterSelector]
                if (pods != null) {
                    pods.remove(resource.metadata.name)
                    if (pods.isEmpty()) {
                        jobmanagerPods.remove(clusterSelector)
                    }
                }
            }

            resource.metadata.labels.get("role") == "taskmanager" ->  {
                val pods = taskmanagerPods[clusterSelector]
                if (pods != null) {
                    pods.remove(resource.metadata.name)
                    if (pods.isEmpty()) {
                        taskmanagerPods.remove(clusterSelector)
                    }
                }
            }
        }
    }

    fun onJobDeletedAll() {
        bootstrapJobs.clear()
    }

    fun onServiceDeletedAll() {
        services.clear()
    }

    fun onPodDeletedAll() {
        jobmanagerPods.clear()
        taskmanagerPods.clear()
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

    private fun acceptClusterSelector(clusterSelector: ClusterSelector) =
        clusterSelector.namespace == namespace && clusterSelector.name == clusterName
}