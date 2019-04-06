package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.JobSubmitConfig
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.CoreV1Api
import java.io.File

class RunSidecar {
    fun run(kubeConfig: String, submitConfig: JobSubmitConfig) {
        Configuration.setDefaultApiClient(CommandUtils.createKubernetesClient(kubeConfig))

        val coreApi = CoreV1Api()

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

            val jobmanagerPort = service.spec.ports.filter { it.name.equals("ui") }.map { it.port }.first()

            val jobmanagerHost = service.spec.clusterIP

            println("Found service ${service.metadata.name}")

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

                println("Found pod ${pod.metadata.name}")

                val api = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort)

                val result = api.uploadJar(File(submitConfig.jarPath))

                println("Jar uploaded...")

                println(Gson().toJson(result))

                if (result.status.name.equals("SUCCESS")) {
                    val response = api.runJar(
                        result.filename.substringAfterLast(delimiter = "/"),
                        false,
                        submitConfig.savepoint,
                        submitConfig.arguments,
                        null,
                        submitConfig.className,
                        submitConfig.parallelism
                    )

                    println(Gson().toJson(response))

                    println("Job started...")

                    while (true) {
                        val jobs = CommandUtils.flinkApi(host = jobmanagerHost, port = jobmanagerPort).jobs

                        jobs.jobs.toList().map { job -> Gson().toJson(job) }.forEach { println(it) }

                        Thread.sleep(10000)
                    }
                }
            } else {
                throw RuntimeException("Pod not found")
            }
        } else {
            throw RuntimeException("Service not found")
        }
    }
}
