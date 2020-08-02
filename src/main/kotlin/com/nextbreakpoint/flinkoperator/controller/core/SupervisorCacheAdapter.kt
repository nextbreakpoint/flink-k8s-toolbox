package com.nextbreakpoint.flinkoperator.controller.core

import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import io.kubernetes.client.util.Watchable
import org.apache.log4j.Logger
import java.net.SocketTimeoutException
import kotlin.concurrent.thread

class SupervisorCacheAdapter(
    private val kubeClient: KubeClient,
    private val supervisorCache: SupervisorCache,
    private val backoffTime: Long = 5000L
) {
    companion object {
        private val logger: Logger = Logger.getLogger(SupervisorCacheAdapter::class.simpleName)
    }

    fun watchClusters(namespace: String) =
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchFlickClusters(namespace)
            }, { resource ->
                supervisorCache.onFlinkClusterChanged(resource)
            }, { resource ->
                supervisorCache.onFlinkClusterDeleted(resource)
            }, {
                supervisorCache.onFlinkClusterDeletedAll()
            })
        }

    fun watchServices(namespace: String) =
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchServices(namespace)
            }, { resource ->
                supervisorCache.onServiceChanged(resource)
            }, { resource ->
                supervisorCache.onServiceDeleted(resource)
            }, {
                supervisorCache.onServiceDeletedAll()
            })
        }

    fun watchPods(namespace: String) =
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchPods(namespace)
            }, { resource ->
                supervisorCache.onPodChanged(resource)
            }, { resource ->
                supervisorCache.onPodDeleted(resource)
            }, {
                supervisorCache.onPodDeletedAll()
            })
        }

    fun watchJobs(namespace: String) =
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchJobs(namespace)
            }, { resource ->
                supervisorCache.onJobChanged(resource)
            }, { resource ->
                supervisorCache.onJobDeleted(resource)
            }, {
                supervisorCache.onJobDeletedAll()
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
