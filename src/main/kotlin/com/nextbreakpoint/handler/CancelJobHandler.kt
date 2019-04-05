package com.nextbreakpoint.handler

import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.flinkclient.model.QueueStatus
import com.nextbreakpoint.flinkclient.model.SavepointTriggerRequestBody
import com.nextbreakpoint.model.JobCancelConfig
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.CoreV1Api
import java.net.URL

object CancelJobHandler {
    fun execute(cancelConfig: JobCancelConfig): String {
        val coreApi = CoreV1Api()

        val kubernetesHost = URL(Configuration.getDefaultApiClient().basePath).host

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

            println("Found service ${service.metadata.name}")

            val jobmanagerPort = service.spec.ports.filter { it.name.equals("ui") }.map { it.nodePort }.first()

            val pods = coreApi.listNamespacedPod(
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

            if (!pods.items.isEmpty()) {
                val pod = pods.items.get(0)

                println("Found pod ${pod.metadata.name}")

                val flinkApi = CommandUtils.flinkApi(host = kubernetesHost, port = jobmanagerPort)

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

                    //                    val operation = flinkApi.createJobSavepoint(savepointTriggerRequestBody(false), cancelConfig.jobId)
                    //
                    //                    while (true) {
                    //                        val operationStatus = flinkApi.getJobSavepointStatus(cancelConfig.jobId, operation.requestId.toString());
                    //
                    //                        if (operationStatus.status.id.equals(QueueStatus.IdEnum.COMPLETED)) {
                    //                            break
                    //                        }
                    //
                    //                        Thread.sleep(5000)
                    //                    }

                    flinkApi.terminateJob(cancelConfig.jobId, "stop")
                }

                println("done")

                return "{\"status\":\"SUCCESS\"}"
            } else {
                throw RuntimeException("Pod not found")
            }
        } else {
            throw RuntimeException("Service not found")
        }
    }

    private fun savepointTriggerRequestBody(isCancel: Boolean): SavepointTriggerRequestBody {
        val requestBody = SavepointTriggerRequestBody()
        requestBody.isCancelJob = isCancel
        requestBody.targetDirectory = "file:///var/tmp/savepoints"
        return requestBody
    }
}