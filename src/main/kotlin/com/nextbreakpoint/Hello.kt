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

    val coreApi = CoreV1Api()

    val srvPort8081 = V1ServicePort().protocol("TCP").port(8081).targetPort(IntOrString("ui")).name("ui")
    val srvPort6123 = V1ServicePort().protocol("TCP").port(6123).targetPort(IntOrString("rpc")).name("rpc")
    val srvPort6124 = V1ServicePort().protocol("TCP").port(6124).targetPort(IntOrString("blob")).name("blob")
    val srvPort6125 = V1ServicePort().protocol("TCP").port(6125).targetPort(IntOrString("query")).name("query")

    val port8081 = V1ContainerPort().protocol("TCP").containerPort(8081).name("ui")
    val port6121 = V1ContainerPort().protocol("TCP").containerPort(6121).name("data")
    val port6122 = V1ContainerPort().protocol("TCP").containerPort(6122).name("ipc")
    val port6123 = V1ContainerPort().protocol("TCP").containerPort(6123).name("rpc")
    val port6124 = V1ContainerPort().protocol("TCP").containerPort(6124).name("blob")
    val port6125 = V1ContainerPort().protocol("TCP").containerPort(6125).name("query")

    val flinkImage = "flink:1.7.1-scala_2.11-alpine"

    val rpcAddressEnvVar = V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value("flink-jobmanager")

    val jobmanager = V1Container()
    jobmanager.image = flinkImage
    jobmanager.name = "flink-jobmanager"
    jobmanager.args = listOf("jobmanager")
    jobmanager.ports = listOf(port8081, port6123, port6124, port6125)
    jobmanager.env = listOf(rpcAddressEnvVar)

    val jobmanagerPodSpec = V1PodSpec().containers(listOf(jobmanager))

    val componentLabel = Pair("component", "flink")

    val jobmanagerLabel = Pair("role", "jobmanager")

    val jobmanagerMetadata = V1ObjectMeta().generateName("flink-jobmanager-").labels(mapOf(componentLabel, jobmanagerLabel))

    val jobmanagerTemplate = V1PodTemplateSpec().spec(jobmanagerPodSpec).metadata(jobmanagerMetadata)

    val strategy = V1DeploymentStrategy().type("Recreate")

    val jobmanagerSelector = V1LabelSelector().matchLabels(mapOf(componentLabel, jobmanagerLabel))

    val jobmanagerDeployment = V1Deployment()
        .metadata(jobmanagerMetadata)
        .spec(V1DeploymentSpec()
            .replicas(1)
            .template(jobmanagerTemplate)
            .strategy(strategy)
            .selector(jobmanagerSelector))

    val jobmanagerDeploymentOut = api.createNamespacedDeployment("default", jobmanagerDeployment, null, null, null)

    println("Job Manager deployment created ${jobmanagerDeploymentOut.metadata.name}")

    val jobmanagerServiceSpec = V1ServiceSpec()
        .ports(listOf(srvPort8081, srvPort6123, srvPort6124, srvPort6125))
        .selector(mapOf(componentLabel, jobmanagerLabel))

    val jobmanagerServiceMetadata = V1ObjectMeta().name("flink-jobmanager").labels(mapOf(componentLabel, jobmanagerLabel))

    val jobmanagerService = V1Service().spec(jobmanagerServiceSpec).metadata(jobmanagerServiceMetadata)

    val jobmanagerServiceOut = coreApi.createNamespacedService("default", jobmanagerService, null, null, null)

    println("Job Manager service created ${jobmanagerServiceOut.metadata.name}")

    val taskmanager = V1Container()
    taskmanager.image = flinkImage
    taskmanager.name = "flink-taskmanager"
    taskmanager.args = listOf("taskmanager")
    taskmanager.ports = listOf(port6121, port6122)
    taskmanager.env = listOf(rpcAddressEnvVar)

    val taskmanagerLabel = Pair("role", "taskmanager")

    val taskmanagerSelector = V1LabelSelector().matchLabels(mapOf(componentLabel, taskmanagerLabel))

    val affinityTerm1 = V1PodAffinityTerm()
        .topologyKey("kubernetes.io/hostname")
        .labelSelector(jobmanagerSelector)

    val affinityTerm2 = V1PodAffinityTerm()
        .topologyKey("kubernetes.io/hostname")
        .labelSelector(taskmanagerSelector)

    val terms = listOf(
        V1WeightedPodAffinityTerm().weight(50).podAffinityTerm(affinityTerm1),
        V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(affinityTerm2)
    )

    val affinity = V1Affinity()
        .podAntiAffinity(V1PodAntiAffinity()
            .preferredDuringSchedulingIgnoredDuringExecution(terms))

    val taskmanagerPodSpec = V1PodSpec().containers(listOf(taskmanager)).affinity(affinity)

    val taskmanagerMetadata = V1ObjectMeta().generateName("flink-taskmanager-").labels(mapOf(componentLabel, taskmanagerLabel))

    val taskmanagerTemplate = V1PodTemplateSpec().spec(taskmanagerPodSpec).metadata(taskmanagerMetadata)

    val taskmanagerDeployment = V1Deployment()
        .metadata(taskmanagerMetadata)
        .spec(V1DeploymentSpec()
            .replicas(1)
            .template(taskmanagerTemplate)
            .strategy(strategy)
            .selector(taskmanagerSelector))

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

