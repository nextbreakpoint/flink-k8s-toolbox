package com.nextbreakpoint.handler

import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.flinkclient.model.QueueStatus
import com.nextbreakpoint.flinkclient.model.SavepointTriggerRequestBody
import com.nextbreakpoint.model.JobCancelConfig
import io.kubernetes.client.apis.CoreV1Api

object CancelJobHandler {
    fun execute(portForward: Int?, useNodePort: Boolean, cancelConfig: JobCancelConfig): String {
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
                cancelConfig.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${cancelConfig.descriptor.name},environment=${cancelConfig.descriptor.environment},role=jobmanager",
                1,
                null,
                30,
                null
            )

            if (!services.items.isEmpty()) {
                val service = services.items.get(0)

                println("Found JobManager ${service.metadata.name}")

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

        if (cancelConfig.savepoint) {
            println("Cancelling jobs with savepoint...")

            val operation =
                flinkApi.createJobSavepoint(
                    savepointTriggerRequestBody(
                        true
                    ), cancelConfig.jobId)

            while (true) {
                val operationStatus =
                    flinkApi.getJobSavepointStatus(cancelConfig.jobId, operation.requestId.toString());

                if (operationStatus.status.id.equals(QueueStatus.IdEnum.COMPLETED)) {
                    break
                }

                Thread.sleep(5000)
            }
        } else {
            println("Cancelling jobs...")

//            val operation = flinkApi.createJobSavepoint(savepointTriggerRequestBody(false), cancelConfig.jobId)
//
//            while (true) {
//                val operationStatus = flinkApi.getJobSavepointStatus(cancelConfig.jobId, operation.requestId.toString());
//
//                if (operationStatus.status.id.equals(QueueStatus.IdEnum.COMPLETED)) {
//                    break
//                }
//
//                Thread.sleep(5000)
//            }

            flinkApi.terminateJob(cancelConfig.jobId, "stop")
        }

        println("done")

        return "{\"status\":\"SUCCESS\"}"
    }

    private fun savepointTriggerRequestBody(isCancel: Boolean): SavepointTriggerRequestBody {
        val requestBody = SavepointTriggerRequestBody()
        requestBody.isCancelJob = isCancel
        requestBody.targetDirectory = "file:///var/tmp/savepoints"
        return requestBody
    }
}