package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.FlinkOptions
import com.nextbreakpoint.flinkclient.api.FlinkApi
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit

object Flink {
    private val logger = Logger.getLogger(Flink::class.simpleName)

    fun find(flinkOptions: FlinkOptions, namespace: String, clusterName: String): FlinkApi {
        var jobmanagerHost = flinkOptions.hostname ?: "localhost"
        var jobmanagerPort = flinkOptions.portForward ?: 8081

        if (flinkOptions.hostname == null && flinkOptions.portForward == null && flinkOptions.useNodePort) {
            val nodes = Kubernetes.coreApi.listNode(
                false,
                null,
                null,
                null,
                null,
                1,
                null,
                30,
                null
            )

            if (!nodes.items.isEmpty()) {
                nodes.items.get(0).status.addresses.filter {
                    it.type.equals("InternalIP")
                }.map {
                    it.address
                }.firstOrNull()?.let {
                    jobmanagerHost = it
                }
            } else {
                throw RuntimeException("Node not found")
            }
        }

        if (flinkOptions.portForward == null) {
            val call = Kubernetes.objectApi.getNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                clusterName,
                null,
                null
            )

            val response = call.execute()

            if (!response.isSuccessful) {
                throw RuntimeException("Can't fetch custom object $clusterName")
            }

            val flinkCluster = FlinkClusterResource.parse(response.body().source().readUtf8Line())

            val clusterId = flinkCluster.metadata.uid

            val services = Kubernetes.coreApi.listNamespacedService(
                namespace,
                null,
                null,
                null,
                null,
                "name=$clusterName,uid=$clusterId,role=jobmanager",
                1,
                null,
                30,
                null
            )

            if (!services.items.isEmpty()) {
                val service = services.items.get(0)

                logger.debug("Found JobManager service ${service.metadata.name}")

                if (flinkOptions.useNodePort) {
                    service.spec.ports.filter {
                        it.name.equals("ui")
                    }.filter {
                        it.nodePort != null
                    }.map {
                        it.nodePort
                    }.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                } else {
                    service.spec.ports.filter {
                        it.name.equals("ui")
                    }.filter {
                        it.port != null
                    }.map {
                        it.port
                    }.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                    jobmanagerHost = service.spec.clusterIP
                }
            } else {
                throw RuntimeException("JobManager service not found (name=$clusterName, id=$clusterId)")
            }

            val pods = Kubernetes.coreApi.listNamespacedPod(
                namespace,
                null,
                null,
                null,
                null,
                "name=$clusterName,uid=$clusterId,role=jobmanager",
                1,
                null,
                30,
                null
            )

            if (!pods.items.isEmpty()) {
                val pod = pods.items.get(0)

                logger.debug("Found JobManager pod ${pod.metadata.name}")
            } else {
                throw RuntimeException("JobManager pod not found (name=$clusterName, id=$clusterId)")
            }
        }

        logger.debug("Connect to Flink using host $jobmanagerHost and port $jobmanagerPort")

        return createFlinkClient(host = jobmanagerHost, port = jobmanagerPort)
    }

    private fun createFlinkClient(host: String = "localhost", port: Int = 8081): FlinkApi {
        val flinkApi = FlinkApi()
        flinkApi.apiClient.basePath = "http://$host:$port"
        flinkApi.apiClient.httpClient.setConnectTimeout(20000, TimeUnit.MILLISECONDS)
        flinkApi.apiClient.httpClient.setWriteTimeout(30000, TimeUnit.MILLISECONDS)
        flinkApi.apiClient.httpClient.setReadTimeout(30000, TimeUnit.MILLISECONDS)
//        flinkApi.apiClient.isDebugging = true
        return flinkApi
    }
}