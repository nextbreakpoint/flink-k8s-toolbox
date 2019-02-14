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
import io.kubernetes.client.Exec
import com.google.common.io.ByteStreams
import java.util.concurrent.TimeUnit


fun main(args: Array<String>) {
    if (args.size < 2) {
        return
    }

    val kubeConfigPath = args[0]

    val flinkImageName = args[1]

    val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

    Configuration.setDefaultApiClient(client)

//    val serviceName = createFlinkCluster(flinkImageName)

    submitJob("flink-jobmanager-n9ckr")

    System.exit(0)
}

fun submitJob(serviceName: String) {
    val coreApi = CoreV1Api()

    val exec = Exec()

    val services = coreApi.listNamespacedService("default", null, null, null, "metadata.name=$serviceName", "role=jobmanager", 1, null, 30, null)

    if (!services.items.isEmpty()) {
        println("Found service ${services.items.get(0).metadata.name}")

        val pods = coreApi.listNamespacedPod("default", null, null, null, null, "role=jobmanager", 1, null, 30, null)

        if (!pods.items.isEmpty()) {
            println("Found pod ${pods.items.get(0).metadata.name}")

            val podName = pods.items.get(0).metadata.name

            val proc = exec.exec("default", podName, arrayOf("ls", "-al"), false, false)

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
                ByteStreams.copy(proc.inputStream, System.out)
            } catch (ex: Exception) {
                ex.printStackTrace()
            }
        })
    val stderr = Thread(
        Runnable {
            try {
                ByteStreams.copy(proc.errorStream, System.out)
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

fun createFlinkCluster(flinkImageName: String) : String {
    val api = AppsV1Api()

    val coreApi = CoreV1Api()

    val srvPort8081 = createServicePort(8081, "ui")
    val srvPort6123 = createServicePort(6123, "rpc")
    val srvPort6124 = createServicePort(6124, "blob")
    val srvPort6125 = createServicePort(6125, "query")

    val port8081 = createContainerPort(8081, "ui")
    val port6121 = createContainerPort(6121, "data")
    val port6122 = createContainerPort(6122, "ipc")
    val port6123 = createContainerPort(6123, "rpc")
    val port6124 = createContainerPort(6124, "blob")
    val port6125 = createContainerPort(6125, "query")

    val componentLabel = Pair("component", "flink")

    val jobmanagerLabel = Pair("role", "jobmanager")

    val taskmanagerLabel = Pair("role", "taskmanager")

    val jobmanagerServiceSpec = V1ServiceSpec()
        .ports(listOf(srvPort8081, srvPort6123, srvPort6124, srvPort6125))
        .selector(mapOf(componentLabel, jobmanagerLabel))

    val jobmanagerServiceMetadata = V1ObjectMeta().generateName("flink-jobmanager-").labels(mapOf(componentLabel, jobmanagerLabel))

    val jobmanagerService = V1Service().spec(jobmanagerServiceSpec).metadata(jobmanagerServiceMetadata)

    val jobmanagerServiceOut = coreApi.createNamespacedService("default", jobmanagerService, null, null, null)

    println("Job Manager service created ${jobmanagerServiceOut.metadata.name}")

    val rpcAddressEnvVar = V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value(jobmanagerServiceOut.metadata.name)

    val jobmanager = V1Container()
    jobmanager.image = flinkImageName
    jobmanager.name = "flink-jobmanager"
    jobmanager.args = listOf("jobmanager")
    jobmanager.ports = listOf(port8081, port6123, port6124, port6125)
    jobmanager.env = listOf(rpcAddressEnvVar)

    val jobmanagerPodSpec = V1PodSpec().containers(listOf(jobmanager))

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

    val taskmanager = V1Container()
    taskmanager.image = flinkImageName
    taskmanager.name = "flink-taskmanager"
    taskmanager.args = listOf("taskmanager")
    taskmanager.ports = listOf(port6121, port6122)
    taskmanager.env = listOf(rpcAddressEnvVar)

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

    return jobmanagerServiceOut.metadata.name
}

private fun createServicePort(port: Int, name: String) = V1ServicePort()
    .protocol("TCP")
    .port(port)
    .targetPort(IntOrString(name))
    .name(name)

private fun createContainerPort(port: Int, name: String) = V1ContainerPort()
    .protocol("TCP")
    .containerPort(port)
    .name(name)

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

