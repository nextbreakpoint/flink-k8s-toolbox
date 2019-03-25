package com.nextbreakpoint.command

import com.google.common.io.ByteStreams.copy
import com.nextbreakpoint.model.JobCancelConfig
import io.kubernetes.client.Configuration
import io.kubernetes.client.Exec
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream
import java.util.concurrent.TimeUnit

class CancelJob {
    fun run(kubeConfigPath: String, cancelConfig: JobCancelConfig) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

        Configuration.setDefaultApiClient(client)

        val coreApi = CoreV1Api()

        val exec = Exec()

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
            println("Found service ${services.items.get(0).metadata.name}")

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
                println("Found pod ${pods.items.get(0).metadata.name}")

                val podName = pods.items.get(0).metadata.name

                if (cancelConfig.savepoint) {
                    println("Cancelling job with savepoint...")

                    val proc = exec.exec(cancelConfig.descriptor.namespace, podName, arrayOf(
                        "flink",
                        "cancel",
                        "-s",
                        "-m",
                        "${services.items.get(0).metadata.name}:8081",
                        cancelConfig.jobId
                    ), false, false)

                    processExec(proc)
                } else {
                    println("Cancelling job...")

                    val proc = exec.exec(cancelConfig.descriptor.namespace, podName, arrayOf(
                        "flink",
                        "cancel",
                        "-m",
                        "${services.items.get(0).metadata.name}:8081",
                        cancelConfig.jobId
                    ), false, false)

                    processExec(proc)
                }

                println("done")
            } else {
                println("Pod not found")
            }
        } else {
            println("Service not found")
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
}

