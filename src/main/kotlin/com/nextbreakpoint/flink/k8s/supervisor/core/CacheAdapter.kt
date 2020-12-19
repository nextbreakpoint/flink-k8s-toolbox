package com.nextbreakpoint.flink.k8s.supervisor.core

import com.nextbreakpoint.flink.k8s.common.KubeClient
import io.kubernetes.client.util.Watchable
import java.net.SocketTimeoutException
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.concurrent.thread

class CacheAdapter(
    private val kubeClient: KubeClient,
    private val cache: Cache,
    private val backoffTime: Long = 5000L
) {
    companion object {
        private val logger: Logger = Logger.getLogger(CacheAdapter::class.simpleName)
    }

    fun watchFlinkClusters(namespace: String) =
        thread {
            watchResources("FlinkClusters", namespace, { namespace ->
                kubeClient.watchFlickClusters(namespace)
            }, { resource ->
                cache.onFlinkClusterChanged(resource)
            }, { resource ->
                cache.onFlinkClusterDeleted(resource)
            }, {
                cache.onFlinkClustersReset()
            })
        }

    fun watchFlinkJobs(namespace: String) =
        thread {
            watchResources("FlinkJobs", namespace, { namespace ->
                kubeClient.watchFlinkJobs(namespace)
            }, { resource ->
                cache.onFlinkJobChanged(resource)
            }, { resource ->
                cache.onFlinkJobDeleted(resource)
            }, {
                cache.onFlinkJobsReset()
            })
        }

    fun watchServices(namespace: String) =
        thread {
            watchResources("Services", namespace, { namespace ->
                kubeClient.watchServices(namespace)
            }, { resource ->
                cache.onServiceChanged(resource)
            }, { resource ->
                cache.onServiceDeleted(resource)
            }, {
                cache.onServicesReset()
            })
        }

    fun watchPods(namespace: String) =
        thread {
            watchResources("Pods", namespace, { namespace ->
                kubeClient.watchPods(namespace)
            }, { resource ->
                cache.onPodChanged(resource)
            }, { resource ->
                cache.onPodDeleted(resource)
            }, {
                cache.onPodsReset()
            })
        }

    fun watchJobs(namespace: String) =
        thread {
            watchResources("Jobs", namespace, { namespace ->
                kubeClient.watchJobs(namespace)
            }, { resource ->
                cache.onJobChanged(resource)
            }, { resource ->
                cache.onJobDeleted(resource)
            }, {
                cache.onJobsReset()
            })
        }

    private fun <T> watchResources(
        name: String,
        namespace: String,
        createResourceWatch: (String) -> Watchable<T>,
        onChangeResource: (T) -> Unit,
        onDeleteResource: (T) -> Unit,
        onReloadResources: () -> Unit
    ) {
        logger.info("Starting watch loop for resources: $name")

        try {
            while (!Thread.interrupted()) {
                try {
                    onReloadResources()

                    createResourceWatch(namespace).use {
                        it.forEach { resource ->
                            when (resource.type) {
                                "ADDED", "MODIFIED" -> {
                                    onChangeResource(resource.`object`)
                                }
                                "DELETED" -> {
                                    onDeleteResource(resource.`object`)
                                }
                            }
                        }
                    }
                } catch (e: Exception) {
                    if (e.cause !is SocketTimeoutException) {
                        logger.log(Level.SEVERE, "An error occurred while watching a resource", e)
                    }
                }

                // back off for a while. perhaps we should use an exponential back off delay
                Thread.sleep(backoffTime)
            }
        } catch (e: InterruptedException) {
        }

        logger.info("Watch loop interrupted. Exiting...")
    }
}
