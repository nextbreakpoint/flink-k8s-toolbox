package com.nextbreakpoint.handler

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.JobsListParams
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger

object JobsListHandler {
    private val logger = Logger.getLogger(JobsListHandler::class.simpleName)

    fun execute(portForward: Int?, useNodePort: Boolean, listParams: JobsListParams): String {
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
                listParams.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${listParams.descriptor.name},environment=${listParams.descriptor.environment},role=jobmanager",
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

        val jobs = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort).jobs

        val result = jobs.jobs
            .filter { job -> !listParams.running || job.status.name.equals("RUNNING") }
            .toList()

        return Gson().toJson(result)
    }
}