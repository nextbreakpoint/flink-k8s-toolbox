package com.nextbreakpoint.flinkoperator.common.utils

import com.google.common.io.ByteStreams
import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import io.kubernetes.client.ApiClient
import io.kubernetes.client.Configuration
import io.kubernetes.client.PortForward
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.BatchV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.apis.CustomObjectsApi
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Pod
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import io.kubernetes.client.util.Config
import io.kubernetes.client.util.Watch
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import org.apache.log4j.Logger
import java.io.File
import java.io.FileInputStream
import java.net.ServerSocket
import java.util.concurrent.TimeUnit

object KubernetesUtils {
    private val logger = Logger.getLogger(KubernetesUtils::class.simpleName)

    val objectApi = CustomObjectsApi()
    val batchApi = BatchV1Api()
    val coreApi = CoreV1Api()
    val appsApi = AppsV1Api()

    private val objectApiWatch = CustomObjectsApi()
    private val batchApiWatch = BatchV1Api()
    private val coreApiWatch = CoreV1Api()
    private val appsApiWatch = AppsV1Api()

    fun configure(kubeConfig: String?) {
        Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig, 10000))
        objectApi.apiClient = Configuration.getDefaultApiClient()
        batchApi.apiClient = Configuration.getDefaultApiClient()
        coreApi.apiClient = Configuration.getDefaultApiClient()
        appsApi.apiClient = Configuration.getDefaultApiClient()
        objectApiWatch.apiClient = createKubernetesClient(kubeConfig, 60000)
        batchApiWatch.apiClient = createKubernetesClient(kubeConfig, 60000)
        coreApiWatch.apiClient = createKubernetesClient(kubeConfig, 60000)
        appsApiWatch.apiClient = createKubernetesClient(kubeConfig, 60000)
    }

    fun updateAnnotations(clusterId: ClusterId, annotations: Map<String, String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "annotations" to annotations
            )
        )

        val response = objectApi.patchNamespacedCustomObjectCall(
            "nextbreakpoint.com",
            "v1",
            clusterId.namespace,
            "flinkclusters",
            clusterId.name,
            patch,
            null,
            null
        ).execute()

        if (!response.isSuccessful) {
            throw RuntimeException("Can't update annotations of cluster ${clusterId.name}")
        }
    }

    fun watchFlickClusterResources(namespace: String): Watch<V1FlinkCluster> =
        Watch.createWatch(
            objectApiWatch.apiClient,
            objectApiWatch.listNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                null,
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1FlinkCluster>>() {}.type
        )

    fun watchServiceResources(namespace: String): Watch<V1Service> =
        Watch.createWatch(
            coreApiWatch.apiClient,
            coreApiWatch.listNamespacedServiceCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Service>>() {}.type
        )

    fun watchDeploymentResources(namespace: String): Watch<V1Deployment> =
        Watch.createWatch(
            appsApiWatch.apiClient,
            appsApiWatch.listNamespacedDeploymentCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Deployment>>() {}.type
        )

    fun watchJobResources(namespace: String): Watch<V1Job> =
        Watch.createWatch(
            batchApiWatch.apiClient,
            batchApiWatch.listNamespacedJobCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Job>>() {}.type
        )

    fun watchStatefulSetResources(namespace: String): Watch<V1StatefulSet> =
        Watch.createWatch(
            appsApiWatch.apiClient,
            appsApiWatch.listNamespacedStatefulSetCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1StatefulSet>>() {}.type
        )

    fun watchPermanentVolumeClaimResources(namespace: String): Watch<V1PersistentVolumeClaim> =
        Watch.createWatch(
            coreApiWatch.apiClient,
            coreApiWatch.listNamespacedPersistentVolumeClaimCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1PersistentVolumeClaim>>() {}.type
        )

    @ExperimentalCoroutinesApi
    @Throws(InterruptedException::class)
    fun forwardPort(
        pod: V1Pod?,
        localPort: Int,
        port: Int,
        stop: Channel<Int>
    ): Thread {
        return Thread(
            Runnable {
                var stdout : Thread? = null
                var stdin : Thread? = null
                try {
                    val forwardResult = PortForward().forward(pod, listOf(port))
                    val serverSocket = ServerSocket(localPort)
                    val clientSocket = serverSocket.accept()
                    stop.invokeOnClose {
                        try {
                            clientSocket.close()
                        } catch (e: Exception) {
                        }
                        try {
                            serverSocket.close()
                        } catch (e: Exception) {
                        }
                    }
                    stdout = Thread(
                        Runnable {
                            try {
                                ByteStreams.copy(clientSocket.inputStream, forwardResult.getOutboundStream(port))
                            } catch (ex: Exception) {
                            }
                        })
                    stdin = Thread(
                        Runnable {
                            try {
                                ByteStreams.copy(forwardResult.getInputStream(port), clientSocket.outputStream)
                            } catch (ex: Exception) {
                            }
                        })
                    stdout.start()
                    stdin.start()
                    stdout.join()
                    stdin.interrupt()
                    stdin.join()
                    stdout = null
                    stdin = null
                } catch (e: Exception) {
                    stdout?.interrupt()
                    stdin?.interrupt()
                    logger.error("An error occurred", e)
                } finally {
                    stdout?.join()
                    stdin?.join()
                }
            })
    }

    @Throws(InterruptedException::class)
    fun processExec(proc: Process) {
        var stdout : Thread? = null
        var stderr : Thread? = null
        try {
            stdout = Thread(
                Runnable {
                    try {
                        ByteStreams.copy(proc.inputStream, System.out)
                    } catch (ex: Exception) {
                    }
                })
            stderr = Thread(
                Runnable {
                    try {
                        ByteStreams.copy(proc.errorStream, System.out)
                    } catch (ex: Exception) {
                    }
                })
            stdout.start()
            stderr.start()
            proc.waitFor(60, TimeUnit.SECONDS)
            stdout.join()
            stderr.join()
            stdout = null
            stderr = null
        } catch (e: Exception) {
            stdout?.interrupt()
            stderr?.interrupt()
            logger.error("An error occurred", e)
        } finally {
            stdout?.join()
            stderr?.join()
        }
    }

    private fun createKubernetesClient(kubeConfig: String?, timeout: Long): ApiClient? {
        val client = if (kubeConfig?.isNotBlank() == true) Config.fromConfig(FileInputStream(File(kubeConfig))) else Config.fromCluster()
        client.httpClient.setConnectTimeout(10000, TimeUnit.MILLISECONDS)
        client.httpClient.setWriteTimeout(timeout, TimeUnit.MILLISECONDS)
        client.httpClient.setReadTimeout(timeout, TimeUnit.MILLISECONDS)
//            client.isDebugging = true
        return client
    }

    fun listFlinkClusterResources(objectApi: CustomObjectsApi, namespace: String): List<V1FlinkCluster> {
        val response = objectApi.listNamespacedCustomObjectCall(
            "nextbreakpoint.com",
            "v1",
            namespace,
            "flinkclusters",
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ).execute()

        if (!response.isSuccessful) {
            throw RuntimeException("Can't fetch custom objects")
        }

        return response.body().use {
            CustomResourceUtils.parseV1FlinkClusterList(it.source().readUtf8Line()).items
        }
    }

    fun getFlinkCluster(namespace: String, name: String): V1FlinkCluster {
        val response = objectApi.getNamespacedCustomObjectCall(
            "nextbreakpoint.com",
            "v1",
            namespace,
            "flinkclusters",
            name,
            null,
            null
        ).execute()

        if (!response.isSuccessful) {
            throw RuntimeException("Can't fetch custom object $name")
        }

        return response.body().use {
            CustomResourceUtils.parseV1FlinkCluster(it.source().readUtf8Line())
        }
    }

    fun listJobResources(batchApi: BatchV1Api, namespace: String): List<V1Job> {
        return batchApi.listNamespacedJob(
            namespace,
            null,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }

    fun listServiceResources(coreApi: CoreV1Api, namespace: String): List<V1Service> {
        return coreApi.listNamespacedService(
            namespace,
            null,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }

    fun listDeploymentResources(appsApi: AppsV1Api, namespace: String): List<V1Deployment> {
        return appsApi.listNamespacedDeployment(
            namespace,
            null,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }

    fun listStatefulSetResources(appsApi: AppsV1Api, namespace: String): List<V1StatefulSet> {
        return appsApi.listNamespacedStatefulSet(
            namespace,
            null,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }

    fun listPermanentVolumeClaimResources(coreApi: CoreV1Api, namespace: String): List<V1PersistentVolumeClaim> {
        return coreApi.listNamespacedPersistentVolumeClaim(
            namespace,
            null,
            null,
            null,
            null,
            "component=flink,owner=flink-operator",
            null,
            null,
            5,
            null
        ).items
    }
}
