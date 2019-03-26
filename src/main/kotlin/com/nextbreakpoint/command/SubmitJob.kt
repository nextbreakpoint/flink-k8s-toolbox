package com.nextbreakpoint.command

import com.google.common.io.ByteStreams.copy
import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.model.JobSubmitConfig
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream
import io.kubernetes.client.Exec
import java.lang.RuntimeException
import java.util.concurrent.TimeUnit

class SubmitJob {
    fun run(kubeConfigPath: String, submitConfig: JobSubmitConfig) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

        Configuration.setDefaultApiClient(client)

        val coreApi = CoreV1Api()

        val exec = Exec()

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
            println("Found service ${services.items.get(0).metadata.name}")

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
                println("Found pod ${pods.items.get(0).metadata.name}")

                val podName = pods.items.get(0).metadata.name

                val command = mutableListOf(
                    "flink",
                    "run",
                    "-d"
                )

                if (submitConfig.savepoint.isNotEmpty()) {
                    command.addAll(arrayListOf(
                        "-s",
                        submitConfig.savepoint
                    ))
                }

                command.addAll(arrayListOf(
                    "-m",
                    "${services.items.get(0).metadata.name}:8081",
                    "-p",
                    submitConfig.parallelism.toString(),
                    "-c",
                    submitConfig.className,
                    submitConfig.jarPath
                ))

                command.addAll(
                    submitConfig.arguments
                        .filter { it.first.startsWith("--") }
                        .flatMap { listOf(it.first, it.second) }
                        .toList()
                )

                val proc = exec.exec(submitConfig.descriptor.namespace, podName, command.toTypedArray(), false, false)

                processExec(proc)

                println("done")
            } else {
                throw RuntimeException("Pod not found")
            }
        } else {
            throw RuntimeException("Service not found")
        }
    }

    @Throws(InterruptedException::class)
    private fun processExec(proc: Process) {
        val stdout = Thread(
            Runnable {
                try {
                    copy(proc.inputStream, System.out)
                } catch (ex: Exception) {
                    ex.printStackTrace()
                }
            })
        val stderr = Thread(
            Runnable {
                try {
                    copy(proc.errorStream, System.out)
                } catch (ex: Exception) {
                    ex.printStackTrace()
                }
            })
        stdout.start()
        stderr.start()
        proc.waitFor(30, TimeUnit.SECONDS)
        stdout.join()
        stderr.join()
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

