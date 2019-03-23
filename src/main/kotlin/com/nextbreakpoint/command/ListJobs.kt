package com.nextbreakpoint.command

import com.google.common.io.ByteStreams.copy
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream
import io.kubernetes.client.Exec
import java.util.concurrent.TimeUnit

class ListJobs {
    private val NAMESPACE = "default"

    fun run(kubeConfigPath: String, clusterName: String) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

        Configuration.setDefaultApiClient(client)

        listJobs(clusterName)
    }

    private fun listJobs(clusterName: String) {
        val coreApi = CoreV1Api()

        val exec = Exec()

        val services = coreApi.listNamespacedService(NAMESPACE, null, null, null, null, "cluster=$clusterName,role=jobmanager", 1, null, 30, null)

        if (!services.items.isEmpty()) {
            println("Found service ${services.items.get(0).metadata.name}")

            val pods = coreApi.listNamespacedPod(NAMESPACE, null, null, null, null, "cluster=$clusterName,role=jobmanager", 1, null, 30, null)

            if (!pods.items.isEmpty()) {
                println("Found pod ${pods.items.get(0).metadata.name}")

                val podName = pods.items.get(0).metadata.name

                val proc = exec.exec(NAMESPACE, podName, arrayOf(
                    "flink",
                    "list",
                    "-r",
                    "-m",
                    "${services.items.get(0).metadata.name}:8081"
                ), false, false)

                processExec(proc)

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

