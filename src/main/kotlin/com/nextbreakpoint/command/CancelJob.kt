package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.flinkApi
import com.nextbreakpoint.flinkclient.model.QueueStatus
import com.nextbreakpoint.flinkclient.model.SavepointTriggerRequestBody
import com.nextbreakpoint.model.JobCancelConfig
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.util.Config
import kotlinx.coroutines.ExperimentalCoroutinesApi
import java.io.File
import java.io.FileInputStream
import java.net.URL

class CancelJob {
    @ExperimentalCoroutinesApi
    fun run(kubeConfigPath: String, cancelConfig: JobCancelConfig) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

        Configuration.setDefaultApiClient(client)

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

                val flinkApi = flinkApi(host = kubernetesHost, port = jobmanagerPort)

                if (cancelConfig.savepoint) {
                    println("Cancelling jobs with savepoint...")

                    val operation = flinkApi.createJobSavepoint(savepointTriggerRequestBody(true), cancelConfig.jobId)

                    while (true) {
                        val operationStatus = flinkApi.getJobSavepointStatus(cancelConfig.jobId, operation.requestId.toString());

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
            } else {
                println("Pod not found")
            }
        } else {
            println("Service not found")
        }
    }

    private fun savepointTriggerRequestBody(isCancel: Boolean): SavepointTriggerRequestBody {
        val requestBody = SavepointTriggerRequestBody()
        requestBody.isCancelJob = isCancel
        requestBody.targetDirectory = "file:///var/tmp/savepoints"
        return requestBody
    }
}

