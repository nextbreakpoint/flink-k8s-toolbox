package com.nextbreakpoint.handler

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.JobSubmitParams
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger
import java.io.File

object JobSubmitHandler {
    private val logger = Logger.getLogger(JobSubmitHandler::class.simpleName)

    fun execute(portForward: Int?, useNodePort: Boolean, submitParams: JobSubmitParams): String {
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
                submitParams.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${submitParams.descriptor.name},environment=${submitParams.descriptor.environment},role=jobmanager",
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

                logger.info("Found JobManager ${service.metadata.name}")
            } else {
                throw RuntimeException("JobManager not found")
            }
        }

        val api = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort)

        val result = api.uploadJar(File(submitParams.jarPath))

        logger.info(Gson().toJson(result))

        if (result.status.name.equals("SUCCESS")) {
            val response = api.runJar(
                result.filename.substringAfterLast(delimiter = "/"),
                false,
                submitParams.savepoint,
                submitParams.arguments,
                null,
                submitParams.className,
                submitParams.parallelism
            )

            logger.info(Gson().toJson(response))

            logger.info("done")

            return "{\"status\":\"SUCCESS\"}"
        } else {
            return "{\"status\":\"FAILED\"}"
        }
    }
}