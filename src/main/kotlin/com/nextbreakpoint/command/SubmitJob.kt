package com.nextbreakpoint.command

import com.google.common.io.ByteStreams.copy
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream
import io.kubernetes.client.Exec
import java.util.concurrent.TimeUnit

class SubmitJob {
    private val NAMESPACE = "default"

    fun run(kubeConfigPath: String, clusterName: String) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

        Configuration.setDefaultApiClient(client)

        listClusterPods(clusterName)

        submitJob(clusterName)
    }

    private fun listClusterPods(clusterName: String) {
        val api = CoreV1Api()

        val list = api.listNamespacedPod(NAMESPACE, null, null, null, null, "cluster=$clusterName,role=jobmanager", null, null, 10, null)

        list.items.forEach {
                item -> println("Pod name: ${item.metadata.name}")
        }

        list.items.forEach {
                item -> println("${item.status}")
        }
    }

    private fun submitJob(clusterName: String) {
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
                    "run",
                    "-d",
                    "-m",
                    "${services.items.get(0).metadata.name}:8081",
                    "-c",
                    "com.nextbreakpoint.flink.jobs.GenerateSensorValuesJob",
                    "/maven/com.nextbreakpoint.flinkdemo-0-SNAPSHOT.jar",
                    "--JOB_PARALLELISM",
                    "1",
                    "--BUCKET_BASE_PATH",
                    "file:///var/tmp/flink",
                    "--BOOTSTRAP_SERVERS",
                    "kafka-k8s1:9092,kafka-k8s2:9092,kafka-k8s3:9092",
                    "--TARGET_TOPIC_NAME",
                    "sensors-input"
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

