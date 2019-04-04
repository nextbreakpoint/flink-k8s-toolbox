package com.nextbreakpoint.command

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils.flinkApi
import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.model.JobSubmitConfig
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream
import kotlinx.coroutines.ExperimentalCoroutinesApi
import java.lang.RuntimeException
import java.net.URL
import java.util.concurrent.TimeUnit

class SubmitJob {
    @ExperimentalCoroutinesApi
    fun run(kubeConfigPath: String, submitConfig: JobSubmitConfig) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))
        client.httpClient.setConnectTimeout(20000, TimeUnit.MILLISECONDS)
        client.httpClient.setWriteTimeout(30000, TimeUnit.MILLISECONDS)
        client.httpClient.setReadTimeout(30000, TimeUnit.MILLISECONDS)
        client.isDebugging = true

        Configuration.setDefaultApiClient(client)

        val coreApi = CoreV1Api()

        val kubernetesHost = URL(Configuration.getDefaultApiClient().basePath).host

        listClusterPods(submitConfig.descriptor)

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

            val jobmanagerPort = service.spec.ports.filter { it.name.equals("ui") }.map { it.nodePort }.first()

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

                val api = flinkApi(host = kubernetesHost, port = jobmanagerPort)

                val result = api.uploadJar(File(submitConfig.jarPath))

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
                }

                println("done")
            } else {
                throw RuntimeException("Pod not found")
            }
        } else {
            throw RuntimeException("Service not found")
        }
    }

    private fun listClusterPods(descriptor: ClusterDescriptor) {
        val api = CoreV1Api()

        val list = api.listNamespacedPod(descriptor.namespace, null, null, null, null, "cluster=${descriptor.namespace},environment=${descriptor.environment}", null, null, 10, null)

        list.items.forEach {
            item -> println("Pod name: ${item.metadata.name}")
        }

//        list.items.forEach {
//                item -> println("${item.status}")
//        }
    }
}

