package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.JobSubmitConfig
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger
import java.io.File

class RunSidecarSubmit {
    companion object {
        val logger = Logger.getLogger(RunSidecarSubmit::class.simpleName)
    }

    fun run(portForward: Int?, useNodePort: Boolean, submitConfig: JobSubmitConfig) {
        try {
            logger.info("Launching FlinkSubmit sidecar...")

            val coreApi = CoreV1Api()

            var jobmanagerHost = "localhost"
            var jobmanagerPort = portForward ?: 8081

            if (portForward == null && useNodePort) {
                RunSidecarWatch.logger.info("NodePort enabled")

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

                    logger.info("Found service ${service.metadata.name}")
                } else {
                    throw RuntimeException("Service not found")
                }

                val pods = coreApi.listNamespacedPod(
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

            logger.info("Uploading jar...")

            val result = api.uploadJar(File(submitConfig.jarPath))

            logger.info("File uploaded: ${Gson().toJson(result)}")

            if (result.status.name.equals("SUCCESS")) {
                logger.info("Starting job...")

                val response = api.runJar(
                    result.filename.substringAfterLast(delimiter = "/"),
                    false,
                    submitConfig.savepoint,
                    submitConfig.arguments,
                    null,
                    submitConfig.className,
                    submitConfig.parallelism
                )

                logger.info("Job started: ${Gson().toJson(response)}")

                logger.info("Monitoring status...")

                while (true) {
                    api.jobs.jobs.toList().map { job -> Gson().toJson(job) }.forEach { logger.info(it) }

                    logger.info("Sleeping a bit...")

                    Thread.sleep(10000)
                }
            }
        } catch (e: Exception) {
            logger.error("An error occurred while launching the sidecar", e)
            throw RuntimeException(e)
        }
    }
}
