package com.nextbreakpoint

import com.google.gson.JsonParseException
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.custom.IntOrString
import io.kubernetes.client.models.*
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream

fun main(args: Array<String>) {
    val client = Config.fromConfig(FileInputStream(File(args[0])))

    Configuration.setDefaultApiClient(client)

    if (args[1].equals("create")) {
        createFlinkCluster()
    }

    if (args[1].equals("list")) {
        printAllPods()
    }

//    deletePod(pod.metadata.name)
}

fun createFlinkCluster() {
    val api = AppsV1Api()

    val srvPort8081 = V1ServicePort().nodePort(8081).name("ui")
    val srvPort6123 = V1ServicePort().nodePort(6123).name("rpc")
    val srvPort6124 = V1ServicePort().nodePort(6124).name("query")

    val port8081 = V1ContainerPort().containerPort(8081)
    val port6121 = V1ContainerPort().containerPort(6121)
    val port6122 = V1ContainerPort().containerPort(6122)
    val port6123 = V1ContainerPort().containerPort(6123)
    val port6124 = V1ContainerPort().containerPort(6124)

    val flinkImage = "flink:1.7.1-scala_2.11-alpine"

    val jobmanager = V1Container()
    jobmanager.image = flinkImage
    jobmanager.name = "flink-jobmanager"
    jobmanager.args = listOf("jobmanager")
    jobmanager.ports = listOf(port8081, port6123, port6124)

    val jobmanagerPodSpec = V1PodSpec().containers(listOf(jobmanager))

    val flink = Pair("component", "flink")

    val jobmanagerMetadata = V1ObjectMeta().generateName("flink-jobmanager-").labels(mapOf(flink))

    val taskmanagerMetadata = V1ObjectMeta().generateName("flink-taskmanager-").labels(mapOf(flink))

    val jobmanagerTemplate = V1PodTemplateSpec().spec(jobmanagerPodSpec).metadata(jobmanagerMetadata)

    val strategy = V1DeploymentStrategy().type("Recreate")

    val selector = V1LabelSelector().matchLabels(mapOf(flink))

    val jobmanagerDeployment = V1Deployment()
        .metadata(jobmanagerMetadata)
        .spec(V1DeploymentSpec()
            .replicas(1)
            .template(jobmanagerTemplate)
            .strategy(strategy)
            .selector(selector))

    val jobmanagerDeploymentOut = api.createNamespacedDeployment("default", jobmanagerDeployment, null, null, null)

    println("Job Manager deployment created ${jobmanagerDeploymentOut.metadata.name}")

    val jobmanagerServiceSpec = V1ServiceSpec()
        .ports(listOf(srvPort8081, srvPort6123, srvPort6124))
        .selector(mapOf(flink))
        .type("NodePort")
//        .externalIPs(listOf("192.168.1.10", "192.168.1.20", "192.168.1.30"))

    val jobmanagerService = V1Service().spec(jobmanagerServiceSpec).metadata(jobmanagerMetadata)

    val coreApi = CoreV1Api()

    val jobmanagerServiceOut = coreApi.createNamespacedService("default", jobmanagerService, null, null, null)

    println("Job Manager service created ${jobmanagerServiceOut.metadata.name}")

    val envVar = V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value(jobmanagerServiceOut.metadata.name)

//    val envVar = V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value("192.168.1.10")

    val taskmanager = V1Container()
    taskmanager.image = flinkImage
    taskmanager.name = "flink-taskmanager"
    taskmanager.args = listOf("taskmanager")
    taskmanager.ports = listOf(port6121, port6122)
    taskmanager.env = listOf(envVar)

    val taskmanagerPodSpec = V1PodSpec().containers(listOf(taskmanager))

    val taskmanagerTemplate = V1PodTemplateSpec().spec(taskmanagerPodSpec).metadata(taskmanagerMetadata)

    val taskmanagerDeployment = V1Deployment()
        .metadata(taskmanagerMetadata)
        .spec(V1DeploymentSpec()
            .replicas(2)
            .template(taskmanagerTemplate)
            .strategy(strategy)
            .selector(selector))

    val taskmanagerDeploymentOut = api.createNamespacedDeployment("default", taskmanagerDeployment, null, null, null)

    println("Task Manager deployment created ${taskmanagerDeploymentOut.metadata.name}")
}

private fun printAllPods() {
    val api = CoreV1Api()

    val list = api.listPodForAllNamespaces(null, null, null, null, null, null, null, 10, null)

    list.items.forEach {
        item -> println("Pod name: ${item.metadata.name}")
    }

    list.items.forEach {
            item -> println("${item.status}")
    }
}

private fun deletePod(name: String) {
    try {
        val api = CoreV1Api()

        val status = api.deleteNamespacedPod(name, "default", V1DeleteOptions(), "true", null, null, null, null)

        println("Response status: ${status.reason}")

        status.details.causes.forEach { println(it.message) }
    } catch (e : JsonParseException) {
        // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
    }
}

