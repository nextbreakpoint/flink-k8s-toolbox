package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.WatchParams
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger

class RunSidecarWatch {
    companion object {
        val logger = Logger.getLogger(RunSidecarWatch::class.simpleName)
    }

    fun run(portForward: Int?, useNodePort: Boolean, watchParams: WatchParams) {
        try {
            logger.info("Launching FlinkController sidecar...")

            val coreApi = CoreV1Api()

            var jobmanagerHost = "localhost"
            var jobmanagerPort = portForward ?: 8081

            if (portForward == null && useNodePort) {
                logger.info("NodePort enabled")

                val nodes = coreApi.listNode(
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

            if (portForward == null) {
                logger.info("Looking for pod...")

                val services = coreApi.listNamespacedService(
                    watchParams.descriptor.namespace,
                    null,
                    null,
                    null,
                    null,
                    "cluster=${watchParams.descriptor.name},environment=${watchParams.descriptor.environment},role=jobmanager",
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

                    logger.info("Found service ${service.metadata.name}")
                } else {
                    throw RuntimeException("Service not found")
                }

                val pods = coreApi.listNamespacedPod(
                    watchParams.descriptor.namespace,
                    null,
                    null,
                    null,
                    null,
                    "cluster=${watchParams.descriptor.name},environment=${watchParams.descriptor.environment},role=jobmanager",
                    1,
                    null,
                    30,
                    null
                )

                if (!pods.items.isEmpty()) {
                    val pod = pods.items.get(0)

                    logger.info("Found pod ${pod.metadata.name}")
                } else {
                    throw RuntimeException("Pod not found")
                }
            }

            if (portForward != null) {
                logger.info("Port Forward enabled")
            }

            val api = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort)

            logger.info("Monitoring status...")

            while (true) {
                api.jobs.jobs.toList().map { job -> Gson().toJson(job) }.forEach { logger.info(it) }

                logger.info("Sleeping for a while...")

                Thread.sleep(10000)
            }
        } catch (e: Exception) {
            logger.error("An error occurred while launching the sidecar", e)
            throw RuntimeException(e)
        }
    }
}
