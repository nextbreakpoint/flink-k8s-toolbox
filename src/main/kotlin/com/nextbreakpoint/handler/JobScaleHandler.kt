package com.nextbreakpoint.handler

import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.JobScaleParams
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger

object JobScaleHandler {
    private val logger = Logger.getLogger(JobScaleHandler::class.simpleName)

    fun execute(portForward: Int?, useNodePort: Boolean, scaleParams: JobScaleParams): String {
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
                scaleParams.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${scaleParams.descriptor.name},environment=${scaleParams.descriptor.environment},role=jobmanager",
                1,
                null,
                30,
                null
            )

            if (!services.items.isEmpty()) {
                val service = services.items.get(0)

                logger.info("Found JobManager ${service.metadata.name}")

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
            } else {
                throw RuntimeException("JobManager not found")
            }
        }

        val flinkApi = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort)

        logger.info("Rescaling job...")

        val operation = flinkApi.triggerJobRescaling(scaleParams.jobId, scaleParams.parallelism)

        logger.info("done")

        return "{\"status\":\"SUCCESS\",\"requestId\":\"${operation.requestId}\"}"
    }
}