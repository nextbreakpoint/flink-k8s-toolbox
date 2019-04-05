package com.nextbreakpoint.handler

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils
import com.nextbreakpoint.model.JobListConfig
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.CoreV1Api
import java.net.URL

object ListJobsHandler {
    fun execute(listConfig: JobListConfig): String {
        val coreApi = CoreV1Api()

        val kubernetesHost = URL(Configuration.getDefaultApiClient().basePath).host

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

            println("Found service ${service.metadata.name}")

            val jobmanagerPort = service.spec.ports.filter { it.name.equals("ui") }.map { it.nodePort }.first()

            val pods = coreApi.listNamespacedPod(
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

            if (!pods.items.isEmpty()) {
                val pod = pods.items.get(0)

                println("Found pod ${pod.metadata.name}")

                val jobs = CommandUtils.flinkApi(host = kubernetesHost, port = jobmanagerPort).jobs

                val result = jobs.jobs
                    .filter { job -> !listConfig.running || job.status.name.equals("RUNNING") }
                    .toList()

                result.map { job -> Gson().toJson(job) }.forEach { println(it) }

                println("done")

                return Gson().toJson(result)
            } else {
                throw RuntimeException("Pod not found")
            }
        } else {
            throw RuntimeException("Service not found")
        }
    }
}