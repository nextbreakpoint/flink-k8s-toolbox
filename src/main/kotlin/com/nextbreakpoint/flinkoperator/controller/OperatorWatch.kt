package com.nextbreakpoint.flinkoperator.controller

import com.google.gson.Gson
import com.nextbreakpoint.flinkoperator.common.utils.KubernetesContext
import io.kubernetes.client.util.Watch
import io.vertx.core.Context
import org.apache.log4j.Logger
import java.net.SocketTimeoutException
import kotlin.concurrent.thread

class OperatorWatch(val gson: Gson, val kubernetesContext: KubernetesContext) {
    companion object {
        private val logger: Logger = Logger.getLogger(OperatorWatch::class.simpleName)
    }

    fun watchFlinkClusters(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                kubernetesContext.watchFlickClusters(it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/flinkcluster/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/flinkcluster/delete", gson.toJson(resource))
                }
            }, { namespace ->
                logger.info("Refresh FlinkClusters resources...")
                context.runOnContext {
                    try {
                        context.owner().eventBus().publish("/resource/flinkcluster/deleteAll", "")
                        val resources = kubernetesContext.listFlinkClusters(namespace)
                        resources.forEach { resource ->
                            context.owner().eventBus().publish("/resource/flinkcluster/change", gson.toJson(resource))
                        }
                    } catch (e: Exception) {
                        logger.error("An error occurred while listing resources", e)
                    }
                }
            })
        }
    }

    fun watchServices(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                kubernetesContext.watchServices(it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/service/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/service/delete", gson.toJson(resource))
                }
            }, { namespace ->
                logger.info("Refresh Services resources...")
                context.runOnContext {
                    try {
                        context.owner().eventBus().publish("/resource/service/deleteAll", "")
                        val resources = kubernetesContext.listServiceResources(namespace)
                        resources.forEach { resource ->
                            context.owner().eventBus().publish("/resource/service/change", gson.toJson(resource))
                        }
                    } catch (e: Exception) {
                        logger.error("An error occurred while listing resources", e)
                    }
                }
            })
        }
    }

    fun watchDeployments(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                kubernetesContext.watchDeployments(it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/deployment/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/deployment/delete", gson.toJson(resource))
                }
            }, { namespace ->
                logger.info("Refresh Deployments resources...")
                context.runOnContext {
                    try {
                        context.owner().eventBus().publish("/resource/deployment/deleteAll", "")
                        val resources = kubernetesContext.listDeploymentResources(namespace)
                        resources.forEach { resource ->
                            context.owner().eventBus().publish("/resource/deployment/change", gson.toJson(resource))
                        }
                    } catch (e: Exception) {
                        logger.error("An error occurred while listing resources", e)
                    }
                }
            })
        }
    }

    fun watchJobs(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                kubernetesContext.watchJobs(it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/job/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/job/delete", gson.toJson(resource))
                }
            }, { namespace ->
                logger.info("Refresh Jobs resources...")
                context.runOnContext {
                    try {
                        context.owner().eventBus().publish("/resource/job/deleteAll", "")
                        val resources = kubernetesContext.listJobResources(namespace)
                        resources.forEach { resource ->
                            context.owner().eventBus().publish("/resource/job/change", gson.toJson(resource))
                        }
                    } catch (e: Exception) {
                        logger.error("An error occurred while listing resources", e)
                    }
                }
            })
        }
    }

    fun watchStatefulSets(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                kubernetesContext.watchStatefulSets(it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/statefulset/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/statefulset/delete", gson.toJson(resource))
                }
            }, { namespace ->
                logger.info("Refresh StatefulSets resources...")
                context.runOnContext {
                    try {
                        context.owner().eventBus().publish("/resource/statefulset/deleteAll", "")
                        val resources = kubernetesContext.listStatefulSetResources(namespace)
                        resources.forEach { resource ->
                            context.owner().eventBus().publish("/resource/statefulset/change", gson.toJson(resource))
                        }
                    } catch (e: Exception) {
                        logger.error("An error occurred while listing resources", e)
                    }
                }
            })
        }
    }

    fun watchPersistentVolumeClaims(context: Context, namespace: String) {
        thread {
            watchResources(namespace, {
                kubernetesContext.watchPermanentVolumeClaims(it)
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/persistentvolumeclaim/change", gson.toJson(resource))
                }
            }, { resource ->
                context.runOnContext {
                    context.owner().eventBus().publish("/resource/persistentvolumeclaim/delete", gson.toJson(resource))
                }
            }, { namespace ->
                logger.info("Refresh PersistentVolumeClaims resources...")
                context.runOnContext {
                    try {
                        context.owner().eventBus().publish("/resource/persistentvolumeclaim/deleteAll", "")
                        val resources = kubernetesContext.listPermanentVolumeClaimResources(namespace)
                        resources.forEach { resource ->
                            context.owner().eventBus().publish("/resource/persistentvolumeclaim/change", gson.toJson(resource))
                        }
                    } catch (e: Exception) {
                        logger.error("An error occurred while listing resources", e)
                    }
                }
            })
        }
    }

    private fun <T> watchResources(
        namespace: String,
        createResourceWatch: (String) -> Watch<T>,
        onChangeResource: (T) -> Unit,
        onDeleteResource: (T) -> Unit,
        onReloadResources: (String) -> Unit
    ) {
        while (true) {
            try {
                onReloadResources(namespace)
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
