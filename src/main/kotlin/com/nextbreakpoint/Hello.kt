package com.nextbreakpoint

import com.google.gson.JsonParseException
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.models.*
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream

private val configPath = "/Users/andrea/Desktop/kubernetes-playground/admin.conf"

fun main(args: Array<String>) {
    val client = Config.fromConfig(FileInputStream(File(configPath)))

    Configuration.setDefaultApiClient(client)

    if (args[0].equals("create")) {
        createFlinkCluster(AppsV1Api())
    }

    if (args[0].equals("list")) {
        printAllPods(CoreV1Api())
    }

//    deletePod(CoreV1Api(), pod.metadata.name)
}

fun createFlinkCluster(api: AppsV1Api) {
    val port8081 = V1ContainerPort().containerPort(8081).hostPort(8081)
    val port6123 = V1ContainerPort().containerPort(6123)
    val port6121 = V1ContainerPort().containerPort(6121)
    val port6122 = V1ContainerPort().containerPort(6122)
    val envVar = V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value("flink-jobmanager")
    val flinkImage = "flink:1.7.1-scala_2.11-alpine"

    val jobmanager = V1Container()
    jobmanager.image = flinkImage
    jobmanager.name = "flink-jobmanager"
    jobmanager.args = listOf("jobmanager")
    jobmanager.ports = listOf(port8081, port6123)

    val taskmanager = V1Container()
    taskmanager.image = flinkImage
    taskmanager.name = "flink-taskmanager"
    taskmanager.args = listOf("taskmanager")
    taskmanager.ports = listOf(port6121, port6122)
    taskmanager.env = listOf(envVar)

    val jobmanagerPodSpec = V1PodSpec()
        .hostname("flink-jobmanager")
        .containers(listOf(jobmanager))

    val taskmanagerPodSpec = V1PodSpec()
        .hostname("flink-taskmanager")
        .containers(listOf(taskmanager))

    val flink = Pair("component", "flink")

    val jobmanagerMetadata = V1ObjectMeta().generateName("flink-jobmanager-").labels(mapOf(flink))

    val taskmanagerMetadata = V1ObjectMeta().generateName("flink-taskmanager-").labels(mapOf(flink))

    val jobmanagerTemplate = V1PodTemplateSpec().spec(jobmanagerPodSpec).metadata(jobmanagerMetadata)

    val taskmanagerTemplate = V1PodTemplateSpec().spec(taskmanagerPodSpec).metadata(taskmanagerMetadata)

    val strategy = V1DeploymentStrategy().type("Recreate")

    val selector = V1LabelSelector().matchLabels(mapOf(flink))

    val jobmanagerDeployment = V1Deployment().metadata(jobmanagerMetadata).spec(V1DeploymentSpec().replicas(1).template(jobmanagerTemplate).strategy(strategy).selector(selector))

    val taskmanagerDeployment = V1Deployment().metadata(taskmanagerMetadata).spec(V1DeploymentSpec().replicas(2).template(taskmanagerTemplate).strategy(strategy).selector(selector))

    val jobmanagerDeploymentOut = api.createNamespacedDeployment("default", jobmanagerDeployment, null, null, null)

    val taskmanagerDeploymentOut = api.createNamespacedDeployment("default", taskmanagerDeployment, null, null, null)

    println("Job Manager created ${jobmanagerDeploymentOut.metadata.name}")

    println("Task Manager created ${taskmanagerDeploymentOut.metadata.name}")
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

