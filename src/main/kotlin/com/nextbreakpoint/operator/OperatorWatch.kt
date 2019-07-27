package com.nextbreakpoint.operator

import com.google.gson.Gson
import com.nextbreakpoint.common.Kubernetes
import io.kubernetes.client.util.Watch
import io.vertx.core.Context
import org.apache.log4j.Logger
import java.net.SocketTimeoutException
import kotlin.concurrent.thread

class OperatorWatch(val gson: Gson) {
    companion object {
        private val logger: Logger = Logger.getLogger(OperatorWatch::class.simpleName)
    }

    fun watchFlinkClusters(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                Kubernetes.watchFlickClusterResources(Kubernetes.objectApi, it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/flinkcluster/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/flinkcluster/delete", gson.toJson(resource))
                }
            })
        }
    }

    fun watchServices(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                Kubernetes.watchServiceResources(Kubernetes.coreApi, it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/service/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/service/delete", gson.toJson(resource))
                }
            })
        }
    }

    fun watchDeployments(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                Kubernetes.watchDeploymentResources(Kubernetes.appsApi, it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/deployment/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/deployment/delete", gson.toJson(resource))
                }
            })
        }
    }

    fun watchJobs(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                Kubernetes.watchJobResources(Kubernetes.batchApi, it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/job/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/job/delete", gson.toJson(resource))
                }
            })
        }
    }

    fun watchStatefulSets(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                Kubernetes.watchStatefulSetResources(Kubernetes.appsApi, it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/statefulset/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/statefulset/delete", gson.toJson(resource))
                }
            })
        }
    }

    fun watchPersistentVolumeClaims(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                Kubernetes.watchPermanentVolumeClaimResources(Kubernetes.coreApi, it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/persistentvolumeclaim/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/persistentvolumeclaim/delete", gson.toJson(resource))
                }
            })
        }
    }

    private fun <T> watchResources(
        namespace: String,
        createResourceWatch: (String) -> Watch<T>,
        onChangeResource: (T) -> Unit,
        onDeleteResource: (T) -> Unit
    ) {
        while (true) {
            try {
                createResourceWatch(namespace).forEach { resource ->
                    when (resource.type) {
                        "ADDED", "MODIFIED" -> {
                            onChangeResource(resource.`object`)
                        }
                        "DELETED" -> {
                            onDeleteResource(resource.`object`)
                        }
                    }
                }
            } catch (e: InterruptedException) {
                break
            } catch (e: RuntimeException) {
                if (!(e.cause is SocketTimeoutException)) {
                    logger.error("An error occurred while watching a resource", e)
                }
            } catch (e: Exception) {
                logger.error("An error occurred while watching a resource", e)
                Thread.sleep(5000L)
            }
        }
    }
}
