package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.informer.ResourceEventHandler
import io.kubernetes.client.informer.SharedInformerFactory
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1Service

class Adapter(
    private val kubeClient: KubeClient,
    private val cache: Cache,
    private val factory: SharedInformerFactory
) {
    fun haveSynced() = haveFlinkClustersSynced() && haveFlinkJobsSynced() && havePodsSynced() && haveServicesSynced() && havePodsSynced() && haveJobsSynced()

    fun start() {
        watchFlinkClusters(cache.namespace)
        watchFlinkJobs(cache.namespace)
        watchServices(cache.namespace)
        watchJobs(cache.namespace)
        watchPods(cache.namespace)

        factory.startAllRegisteredInformers()
    }

    fun stop() {
        factory.stopAllRegisteredInformers()
    }

    private fun haveFlinkClustersSynced() =
        factory.getExistingSharedIndexInformer(V1FlinkCluster::class.java).hasSynced()

    private fun haveFlinkJobsSynced() =
        factory.getExistingSharedIndexInformer(V1FlinkJob::class.java).hasSynced()

    private fun haveServicesSynced() =
        factory.getExistingSharedIndexInformer(V1Service::class.java).hasSynced()

    private fun havePodsSynced() =
        factory.getExistingSharedIndexInformer(V1Pod::class.java).hasSynced()

    private fun haveJobsSynced() =
        factory.getExistingSharedIndexInformer(V1Job::class.java).hasSynced()

    private fun watchFlinkClusters(namespace: String) {
        val informer = kubeClient.createFlinkClustersInformer(factory, namespace)

        informer.addEventHandlerWithResyncPeriod(
            object : ResourceEventHandler<V1FlinkCluster> {
                override fun onAdd(resource: V1FlinkCluster) {
                    cache.onFlinkClusterChanged(resource)
                }

                override fun onUpdate(oldResource: V1FlinkCluster, newResource: V1FlinkCluster) {
                    cache.onFlinkClusterChanged(newResource)
                }

                override fun onDelete(resource: V1FlinkCluster, deletedFinalStateUnknown: Boolean) {
                    cache.onFlinkClusterDeleted(resource)
                }
            }, 5000)
    }

    private fun watchFlinkJobs(namespace: String) {
        val informer = kubeClient.createFlinkJobsInformer(factory, namespace)

        informer.addEventHandlerWithResyncPeriod(
            object : ResourceEventHandler<V1FlinkJob> {
                override fun onAdd(resource: V1FlinkJob) {
                    cache.onFlinkJobChanged(resource)
                }

                override fun onUpdate(oldResource: V1FlinkJob, newResource: V1FlinkJob) {
                    cache.onFlinkJobChanged(newResource)
                }

                override fun onDelete(resource: V1FlinkJob, deletedFinalStateUnknown: Boolean) {
                    cache.onFlinkJobDeleted(resource)
                }
            }, 5000)
    }

    private fun watchServices(namespace: String) {
        val informer = kubeClient.createServicesInformer(factory, namespace)

        informer.addEventHandlerWithResyncPeriod(
            object : ResourceEventHandler<V1Service> {
                override fun onAdd(resource: V1Service) {
                    cache.onServiceChanged(resource)
                }

                override fun onUpdate(oldResource: V1Service, newResource: V1Service) {
                    cache.onServiceChanged(newResource)
                }

                override fun onDelete(resource: V1Service, deletedFinalStateUnknown: Boolean) {
                    cache.onServiceDeleted(resource)
                }
            }, 5000)
    }

    private fun watchPods(namespace: String) {
        val informer = kubeClient.createPodsInformer(factory, namespace)

        informer.addEventHandlerWithResyncPeriod(
            object : ResourceEventHandler<V1Pod> {
                override fun onAdd(resource: V1Pod) {
                    cache.onPodChanged(resource)
                }

                override fun onUpdate(oldResource: V1Pod, newResource: V1Pod) {
                    cache.onPodChanged(newResource)
                }

                override fun onDelete(resource: V1Pod, deletedFinalStateUnknown: Boolean) {
                    cache.onPodDeleted(resource)
                }
            }, 5000)
    }

    private fun watchJobs(namespace: String) {
        val informer = kubeClient.createJobsInformer(factory, namespace)

        informer.addEventHandlerWithResyncPeriod(
            object : ResourceEventHandler<V1Job> {
                override fun onAdd(resource: V1Job) {
                    cache.onJobChanged(resource)
                }

                override fun onUpdate(oldResource: V1Job, newResource: V1Job) {
                    cache.onJobChanged(newResource)
                }

                override fun onDelete(resource: V1Job, deletedFinalStateUnknown: Boolean) {
                    cache.onJobDeleted(resource)
                }
            }, 5000)
    }
}
