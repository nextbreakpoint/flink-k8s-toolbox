package com.nextbreakpoint.handler

import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.flinkclient.model.QueueStatus
import com.nextbreakpoint.flinkclient.model.SavepointTriggerRequestBody
import com.nextbreakpoint.model.JobCancelParams
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger

object JobCancelHandler {
    private val logger = Logger.getLogger(JobCancelHandler::class.simpleName)

    fun execute(portForward: Int?, useNodePort: Boolean, cancelParams: JobCancelParams): String {
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
                cancelParams.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${cancelParams.descriptor.name},environment=${cancelParams.descriptor.environment},role=jobmanager",
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

        if (cancelParams.savepoint) {
            logger.info("Cancelling jobs with savepoint...")

            val operation =
                flinkApi.createJobSavepoint(
                    savepointTriggerRequestBody(
                        true,
                        cancelParams.savepointPath
                    ), cancelParams.jobId)

            while (true) {
                val operationStatus =
                    flinkApi.getJobSavepointStatus(cancelParams.jobId, operation.requestId.toString());

                if (operationStatus.status.id.equals(QueueStatus.IdEnum.COMPLETED)) {
                    break
                }

                Thread.sleep(5000)
            }
        } else {
            logger.info("Cancelling jobs...")

//            val operation = flinkApi.createJobSavepoint(savepointTriggerRequestBody(false), cancelParams.jobId)
//
//            while (true) {
//                val operationStatus = flinkApi.getJobSavepointStatus(cancelParams.jobId, operation.requestId.toString());
//
//                if (operationStatus.status.id.equals(QueueStatus.IdEnum.COMPLETED)) {
//                    break
//                }
//
//                Thread.sleep(5000)
//            }

            flinkApi.terminateJob(cancelParams.jobId, "stop")
        }

        logger.info("done")

        return "{\"status\":\"SUCCESS\"}"
    }

    private fun savepointTriggerRequestBody(isCancel: Boolean, savepointPath: String): SavepointTriggerRequestBody {
        val requestBody = SavepointTriggerRequestBody()
        requestBody.isCancelJob = isCancel
        requestBody.targetDirectory = savepointPath
        return requestBody
    }
}