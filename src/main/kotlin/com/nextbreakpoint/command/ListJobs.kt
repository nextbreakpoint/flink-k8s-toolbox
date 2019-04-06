package com.nextbreakpoint.command

import com.google.common.io.ByteStreams.copy
import com.nextbreakpoint.model.JobListConfig
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream
import io.kubernetes.client.Exec
import java.lang.RuntimeException
import java.util.concurrent.TimeUnit

class ListJobs {
    fun run(kubeConfigPath: String, listConfig: JobListConfig) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

        Configuration.setDefaultApiClient(client)

        val coreApi = CoreV1Api()

        val exec = Exec()

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
            println("Found service ${services.items.get(0).metadata.name}")

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
                println("Found pod ${pods.items.get(0).metadata.name}")

                val podName = pods.items.get(0).metadata.name

                if (listConfig.running) {
                    val proc = exec.exec(listConfig.descriptor.namespace, podName, arrayOf(
                        "flink",
                        "list",
                        "-r",
                        "-m",
                        "${services.items.get(0).metadata.name}:8081"
                    ), false, false)

                    processExec(proc)
                } else {
                    val proc = exec.exec(listConfig.descriptor.namespace, podName, arrayOf(
                        "flink",
                        "list",
                        "-a",
                        "-m",
                        "${services.items.get(0).metadata.name}:8081"
                    ), false, false)

                    processExec(proc)
                }

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
        stdout.interrupt()
        stderr.interrupt()
        stdout.join()
        stderr.join()
    }
}

