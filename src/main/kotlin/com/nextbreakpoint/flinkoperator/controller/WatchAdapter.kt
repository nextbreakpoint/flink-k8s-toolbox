package com.nextbreakpoint.flinkoperator.controller

import com.nextbreakpoint.flinkoperator.common.utils.KubeClient
import com.nextbreakpoint.flinkoperator.controller.core.Cache
import io.kubernetes.client.JSON
import io.kubernetes.client.util.Watch
import org.apache.log4j.Logger
import java.net.SocketTimeoutException
import kotlin.concurrent.thread

class WatchAdapter(val json: JSON, val kubeClient: KubeClient, val cache: Cache) {
    companion object {
        private val logger: Logger = Logger.getLogger(WatchAdapter::class.simpleName)
    }

    fun watchClusters(namespace: String) {
        thread {
            watchResources(namespace, { namespace ->
                kubeClient.watchFlickClusters(namespace)
            }, { resource ->
                cache.onFlinkClusterChanged(resource)
            }, { resource ->
                cache.onFlinkClusterDeleted(resource)
            }, {
                cache.onFlinkClusterDeleteAll()
            })
        }
    }

//    fun watchServices(namespace: String) {
//        thread {
//            watchResources(namespace, { namespace ->
//                kubeClient.watchServices(namespace)
//            }, { resource ->
//                cache.onFlinkClusterChanged(resource)
//            }, { resource ->
//                cache.onFlinkClusterDeleted(resource)
//            }, {
//                cache.onFlinkClusterDeleteAll()
//            })
//        }
//    }

    private fun <T> watchResources(
        namespace: String,
        createResourceWatch: (String) -> Watch<T>,
        onChangeResource: (T) -> Unit,
        onDeleteResource: (T) -> Unit,
        onReloadResources: () -> Unit
    ) {
        while (true) {
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
                Thread.sleep(1000L)
            } catch (e: InterruptedException) {
                break
            } catch (e: RuntimeException) {
                if (e.cause !is SocketTimeoutException) {
                    logger.error("An error occurred while watching a resource", e)
                    Thread.sleep(5000L)
                }
            } catch (e: Exception) {
                logger.error("An error occurred while watching a resource", e)
                Thread.sleep(5000L)
            }
        }
    }
}
