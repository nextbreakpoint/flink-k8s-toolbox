package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.k8s.common.KubeClient
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import io.kubernetes.client.informer.ResourceEventHandler
import io.kubernetes.client.informer.SharedInformerFactory
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod

class Adapter(
    private val kubeClient: KubeClient,
    private val cache: Cache,
    private val factory: SharedInformerFactory
) {
    fun haveSynced() = haveFlinkDeploymentsSynced() && haveFlinkClustersSynced() && haveFlinkJobsSynced() && havePodsSynced() && haveDeploymentsSynced()

    fun start() {
        watchFlinkDeployments(cache.namespace)
        watchFlinkClusters(cache.namespace)
        watchFlinkJobs(cache.namespace)
        watchDeployments(cache.namespace)
        watchPods(cache.namespace)

        factory.startAllRegisteredInformers()
    }

    fun stop() {
        factory.stopAllRegisteredInformers()
    }

    private fun haveFlinkDeploymentsSynced() =
        factory.getExistingSharedIndexInformer(V1FlinkDeployment::class.java).hasSynced()

    private fun haveFlinkClustersSynced() =
        factory.getExistingSharedIndexInformer(V1FlinkCluster::class.java).hasSynced()

    private fun haveFlinkJobsSynced() =
        factory.getExistingSharedIndexInformer(V1FlinkJob::class.java).hasSynced()

    private fun havePodsSynced() =
        factory.getExistingSharedIndexInformer(V1Pod::class.java).hasSynced()

    private fun haveDeploymentsSynced() =
        factory.getExistingSharedIndexInformer(V1Deployment::class.java).hasSynced()

    private fun watchFlinkDeployments(namespace: String) {
        val informer = kubeClient.createFlinkDeploymentsInformer(factory, namespace)

        informer.addEventHandlerWithResyncPeriod(
            object : ResourceEventHandler<V1FlinkDeployment> {
                override fun onAdd(resource: V1FlinkDeployment) {
                    cache.onFlinkDeploymentChanged(resource)
                }

                override fun onUpdate(oldResource: V1FlinkDeployment, newResource: V1FlinkDeployment) {
                    cache.onFlinkDeploymentChanged(newResource)
                }

                override fun onDelete(resource: V1FlinkDeployment, deletedFinalStateUnknown: Boolean) {
                    cache.onFlinkDeploymentDeleted(resource)
                }
            }, 5000)
    }

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

    private fun watchDeployments(namespace: String) {
        val informer = kubeClient.createDeploymentsInformer(factory, namespace)

        informer.addEventHandlerWithResyncPeriod(
            object : ResourceEventHandler<V1Deployment> {
                override fun onAdd(resource: V1Deployment) {
                    cache.onDeploymentChanged(resource)
                }

                override fun onUpdate(oldResource: V1Deployment, newResource: V1Deployment) {
                    cache.onDeploymentChanged(newResource)
                }

                override fun onDelete(resource: V1Deployment, deletedFinalStateUnknown: Boolean) {
                    cache.onDeploymentDeleted(resource)
                }
            }, 5000)
    }
}
