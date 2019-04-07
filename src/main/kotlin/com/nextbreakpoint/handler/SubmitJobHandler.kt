package com.nextbreakpoint.handler

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.JobSubmitConfig
import io.kubernetes.client.apis.CoreV1Api
import java.io.File

object SubmitJobHandler {
    fun execute(portForward: Int?, useNodePort: Boolean, submitConfig: JobSubmitConfig): String {
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
                submitConfig.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${submitConfig.descriptor.name},environment=${submitConfig.descriptor.environment},role=jobmanager",
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

                println("Found JobManager ${service.metadata.name}")
            } else {
                throw RuntimeException("JobManager not found")
            }
        }

        val api = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort)

        val result = api.uploadJar(File(submitConfig.jarPath))

        println(Gson().toJson(result))

        if (result.status.name.equals("SUCCESS")) {
            val response = api.runJar(
                result.filename.substringAfterLast(delimiter = "/"),
                false,
                submitConfig.savepoint,
                submitConfig.arguments,
                null,
                submitConfig.className,
                submitConfig.parallelism
            )

            println(Gson().toJson(response))

            println("done")

            return "{\"status\":\"SUCCESS\"}"
        } else {
            return "{\"status\":\"FAILED\"}"
        }
    }
}