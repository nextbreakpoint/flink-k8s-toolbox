package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import io.kubernetes.client.util.Watchable
import org.apache.log4j.Logger
import java.net.SocketTimeoutException
import kotlin.concurrent.thread

class CacheAdapter(
    private val kubeClient: KubeClient,
    private val cache: Cache,
    private val backoffTime: Long = 5000L
) {
    companion object {
        private val logger: Logger = Logger.getLogger(CacheAdapter::class.simpleName)
    }

    fun watchClusters(namespace: String) =
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchFlickClusters(namespace)
            }, { resource ->
                cache.onFlinkClusterChanged(resource)
            }, { resource ->
                cache.onFlinkClusterDeleted(resource)
            }, {
                cache.onFlinkClusterDeletedAll()
            })
        }

    fun watchJobs(namespace: String) =
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchJobs(namespace)
            }, { resource ->
                cache.onJobChanged(resource)
            }, { resource ->
                cache.onJobDeleted(resource)
            }, {
                cache.onJobDeletedAll()
            })
        }

    fun watchServices(namespace: String) =
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchServices(namespace)
            }, { resource ->
                cache.onServiceChanged(resource)
            }, { resource ->
                cache.onServiceDeleted(resource)
            }, {
                cache.onServiceDeletedAll()
            })
        }

    fun watchStatefuleSets(namespace: String) =
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchStatefulSets(namespace)
            }, { resource ->
                cache.onStatefulSetChanged(resource)
            }, { resource ->
                cache.onStatefulSetDeleted(resource)
            }, {
                cache.onStatefulSetDeletedAll()
            })
        }

    fun watchPersistentVolumeClaims(namespace: String) =
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchPersistentVolumeClaims(namespace)
            }, { resource ->
                cache.onPersistentVolumeClaimChanged(resource)
            }, { resource ->
                cache.onPersistentVolumeClaimDeleted(resource)
            }, {
                cache.onPersistentVolumeClaimDeletedAll()
            })
        }

    private fun <T> watchResources(
        namespace: String,
        createResourceWatch: (String) -> Watchable<T>,
        onChangeResource: (T) -> Unit,
        onDeleteResource: (T) -> Unit,
        onReloadResources: () -> Unit
    ) {
        logger.info("Watch loop started")

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
                        logger.error("An error occurred while watching a resource", e)
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
