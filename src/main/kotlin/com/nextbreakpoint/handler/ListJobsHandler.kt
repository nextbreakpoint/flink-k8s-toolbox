package com.nextbreakpoint.handler

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.JobListConfig
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger

object ListJobsHandler {
    val logger = Logger.getLogger(ListJobsHandler::class.simpleName)

    fun execute(portForward: Int?, useNodePort: Boolean, listConfig: JobListConfig): String {
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
                listConfig.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${listConfig.descriptor.name},environment=${listConfig.descriptor.environment},role=jobmanager",
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
            .filter { job -> !listConfig.running || job.status.name.equals("RUNNING") }
            .toList()

        result.map { job -> Gson().toJson(job) }.forEach { logger.info(it) }

        logger.info("done")

        return Gson().toJson(result)
    }
}