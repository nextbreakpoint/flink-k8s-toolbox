package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.WatchConfig
import io.kubernetes.client.apis.CoreV1Api

class RunSidecarWatch {
    fun run(portForward: Int?, useNodePort: Boolean, watchConfig: WatchConfig) {
        val coreApi = CoreV1Api()

        var jobmanagerHost = "localhost"
        var jobmanagerPort = portForward ?: 8081

        if (portForward == null && useNodePort) {
            val nodes = coreApi.listNode(false,
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

        if (portForward == null) {
            val services = coreApi.listNamespacedService(
                watchConfig.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${watchConfig.descriptor.name},environment=${watchConfig.descriptor.environment},role=jobmanager",
                1,
                null,
                30,
                null
            )

            if (!services.items.isEmpty()) {
                val service = services.items.get(0)

                if (useNodePort) {
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

                println("Found service ${service.metadata.name}")
            } else {
                throw RuntimeException("Service not found")
            }

            val pods = coreApi.listNamespacedPod(
                watchConfig.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${watchConfig.descriptor.name},environment=${watchConfig.descriptor.environment},role=jobmanager",
                1,
                null,
                30,
                null
            )

            if (!pods.items.isEmpty()) {
                val pod = pods.items.get(0)

                println("Found pod ${pod.metadata.name}")
            } else {
                throw RuntimeException("Pod not found")
            }
        }

        val api = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort)

        while (true) {
            api.jobs.jobs.toList().map { job -> Gson().toJson(job) }.forEach { println(it) }

            Thread.sleep(10000)
        }
    }
}
