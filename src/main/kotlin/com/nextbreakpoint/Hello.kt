package com.nextbreakpoint

import com.google.gson.JsonParseException
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.models.*
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream

fun main(args: Array<String>) {
    val api = coreV1Api()

    if (args[0].equals("create")) {
        createFlinkCluster(api)
    }

    if (args[0].equals("list")) {
        printAllPods(api)
    }

//    deletePod(api, pod.metadata.name)
}

private val configPath = "/Users/andrea/Documents/projects/infrastructure/kubernetes-playground/admin.conf"

private fun coreV1Api(): CoreV1Api {
    val client = Config.fromConfig(FileInputStream(File(configPath)))

    Configuration.setDefaultApiClient(client)

    val api = CoreV1Api()

    return api
}

fun createFlinkCluster(api: CoreV1Api) {
    val jobmanager = V1Container()
    jobmanager.image = "flink:1.7.1-scala_2.11-alpine"
    jobmanager.name = "flink-jobmanager"
    jobmanager.args = listOf("jobmanager")
    jobmanager.ports = listOf(V1ContainerPort().containerPort(8081).hostPort(8081), V1ContainerPort().containerPort(6123))

    val taskmanager = V1Container()
    taskmanager.image = "flink:1.7.1-scala_2.11-alpine"
    taskmanager.name = "flink-taskmanager"
    taskmanager.args = listOf("taskmanager")
    taskmanager.ports = listOf(V1ContainerPort().containerPort(6121), V1ContainerPort().containerPort(6122))
    taskmanager.env = listOf(V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value("flink-jobmanager.default.pod.cluster.local"))

    val jobmanagerPodIn = V1Pod()
        .metadata(V1ObjectMeta().generateName("flink-jobmanager-"))
        .spec(V1PodSpec().hostname("flink-jobmanager").containers(listOf(jobmanager)).restartPolicy("Never"))

    val taskmanagerPodIn = V1Pod()
        .metadata(V1ObjectMeta().generateName("flink-taskmanager-"))
        .spec(V1PodSpec().containers(listOf(taskmanager)).restartPolicy("Never"))

    val jobmanagerPodOut = api.createNamespacedPod("default", jobmanagerPodIn, null, null, null)
    println("Job Manager created ${jobmanagerPodOut.metadata.name}")

    val taskmanagerPodOut = api.createNamespacedPod("default", taskmanagerPodIn, null, null, null)
    println("Task Manager created ${taskmanagerPodOut.metadata.name}")
}

private fun printAllPods(api: CoreV1Api) {
    val list = api.listPodForAllNamespaces(null, null, null, null, null, null, null, 10, null)

    list.items.forEach {
        item -> println("Pod name: ${item.metadata.name}")
    }

    list.items.forEach {
            item -> println("${item.status}")
    }
}

private fun deletePod(api: CoreV1Api, name: String) {
    try {
        val status = api.deleteNamespacedPod(name, "default", V1DeleteOptions(), "true", null, null, null, null)

        println("Response status: ${status.reason}")

        status.details.causes.forEach { println(it.message) }
    } catch (e : JsonParseException) {
        // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
    }
}

