package com.nextbreakpoint

import com.google.common.io.ByteStreams.copy
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
import io.kubernetes.client.custom.Quantity
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    if (args.size < 2) {
        return
    }

    val kubeConfigPath = args[0]

    val flinkImageName = args[1]

    val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

    Configuration.setDefaultApiClient(client)

    val serviceName = createFlinkCluster(flinkImageName)

    Thread.sleep(60000)

    submitJob(serviceName)

    Thread.sleep(60000)

    printAllPods()

    deleteFlinkCluster(serviceName)

    System.exit(0)
}

private val NAMESPACE = "default"

private fun submitJob(serviceName: String) {
    val coreApi = CoreV1Api()

    val exec = Exec()

    val services = coreApi.listNamespacedService(NAMESPACE, null, null, null, "metadata.name=$serviceName", "role=jobmanager", 1, null, 30, null)

    if (!services.items.isEmpty()) {
        println("Found service ${services.items.get(0).metadata.name}")

        val pods = coreApi.listNamespacedPod(NAMESPACE, null, null, null, null, "role=jobmanager", 1, null, 30, null)

        if (!pods.items.isEmpty()) {
            println("Found pod ${pods.items.get(0).metadata.name}")

            val podName = pods.items.get(0).metadata.name

            val proc = exec.exec(NAMESPACE, podName, arrayOf("flink", "list", "-r", "-m", "${serviceName}:8081"), false, false)

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

private fun createFlinkCluster(flinkImageName: String) : String {
    val api = AppsV1Api()

    val coreApi = CoreV1Api()

    val environment = "test"

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

    val storageClassName = "standard"

    val jobmanagerStorageSize = "2Gi"

    val taskmanagerStorageSize = "5Gi"

    val pullSecretsReference = V1LocalObjectReference().name("regcred")

    val componentLabel = Pair("component", "flink")

    val jobmanagerLabel = Pair("role", "jobmanager")

    val taskmanagerLabel = Pair("role", "taskmanager")

    val jobmanagerResourceRequirements = V1ResourceRequirements()
        .limits(mapOf("cpu" to Quantity("1"), "memory" to Quantity("600Mi")))
        .requests(mapOf("cpu" to Quantity("0.2"), "memory" to Quantity("200Mi")))

    val taskmanagerResourceRequirements = V1ResourceRequirements()
        .limits(mapOf("cpu" to Quantity("1"), "memory" to Quantity("1200Mi")))
        .requests(mapOf("cpu" to Quantity("0.2"), "memory" to Quantity("200Mi")))

    val jobmanagerSelector = V1LabelSelector().matchLabels(mapOf(componentLabel, jobmanagerLabel))

    val taskmanagerSelector = V1LabelSelector().matchLabels(mapOf(componentLabel, taskmanagerLabel))

    val environmentEnvVar = V1EnvVar().name("FLINK_ENVIRONMENT").value(environment)

    val flinkJobManagerHeap = V1EnvVar().name("FLINK_JM_HEAP").value("512")

    val flinkTaskManagerHeap = V1EnvVar().name("FLINK_TM_HEAP").value("1024")

    val flinkTaskManagerNumberOfTaskSlots = V1EnvVar().name("TASK_MANAGER_NUMBER_OF_TASK_SLOTS").value("1")

    val podNameValueSource = V1EnvVarSource().fieldRef(V1ObjectFieldSelector().fieldPath("metadata.name"))

    val podNamespaceValueSource = V1EnvVarSource().fieldRef(V1ObjectFieldSelector().fieldPath("metadata.namespace"))

    val podNameEnvVar = V1EnvVar().name("POD_NAME").valueFrom(podNameValueSource)

    val podNamespaceEnvVar = V1EnvVar().name("POD_NAMESPACE").valueFrom(podNamespaceValueSource)

    val jobmanagerVolumeMount = V1VolumeMount().mountPath("/var/tmp/data").subPath("data").name("jobmanager")

    val taskmanagerVolumeMount = V1VolumeMount().mountPath("/var/tmp/data").subPath("data").name("taskmanager")

    val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

    val jobmanagerServiceSpec = V1ServiceSpec()
        .ports(listOf(srvPort8081, srvPort6123, srvPort6124, srvPort6125))
        .selector(mapOf(componentLabel, jobmanagerLabel))

    val jobmanagerServiceMetadata = V1ObjectMeta().generateName("flink-jobmanager-").labels(mapOf(componentLabel, jobmanagerLabel))

    val jobmanagerService = V1Service().spec(jobmanagerServiceSpec).metadata(jobmanagerServiceMetadata)

    val jobmanagerServiceOut = coreApi.createNamespacedService(NAMESPACE, jobmanagerService, null, null, null)

    println("Job Manager Service created ${jobmanagerServiceOut.metadata.name}")

    val rpcAddressEnvVar = V1EnvVar().name("JOB_MANAGER_RPC_ADDRESS").value(jobmanagerServiceOut.metadata.name)

    val jobmanager = V1Container()
        .image(flinkImageName)
        .imagePullPolicy("IfNotPresent")
        .name("flink-jobmanager")
        .command(listOf("/custom_entrypoint.sh"))
        .args(listOf("jobmanager"))
        .ports(listOf(port8081, port6123, port6124, port6125))
        .volumeMounts(listOf(jobmanagerVolumeMount))
        .env(listOf(podNameEnvVar, podNamespaceEnvVar, environmentEnvVar, rpcAddressEnvVar, flinkJobManagerHeap))
        .resources(jobmanagerResourceRequirements)

//    val jobmanagerNodeSelector = V1NodeSelector().nodeSelectorTerms(listOf(
//        V1NodeSelectorTerm().matchExpressions(listOf(
//            V1NodeSelectorRequirement().key("kubernetes.io/hostname").operator("in").values(listOf("k8s1")))
//        )
//    ))

    val jobmanagerAffinity = V1Affinity()
//        .nodeAffinity(V1NodeAffinity().requiredDuringSchedulingIgnoredDuringExecution(jobmanagerNodeSelector))
        .podAntiAffinity(V1PodAntiAffinity().preferredDuringSchedulingIgnoredDuringExecution(listOf(
            V1WeightedPodAffinityTerm().weight(50).podAffinityTerm(V1PodAffinityTerm().topologyKey("kubernetes.io/hostname").labelSelector(jobmanagerSelector)),
            V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(V1PodAffinityTerm().topologyKey("kubernetes.io/hostname").labelSelector(taskmanagerSelector))
        )))

    val jobmanagerPodSpec = V1PodSpec().containers(listOf(jobmanager)).imagePullSecrets(listOf(pullSecretsReference)).affinity(jobmanagerAffinity)

    val jobmanagerMetadata = V1ObjectMeta().generateName("flink-jobmanager-").labels(mapOf(componentLabel, jobmanagerLabel))

    val jobmanagerTemplate = V1PodTemplateSpec().spec(jobmanagerPodSpec).metadata(jobmanagerMetadata)

    val jobmanagerVolumeClaim = V1PersistentVolumeClaimSpec()
        .accessModes(listOf("ReadWriteOnce"))
        .volumeName("flink")
        .storageClassName(storageClassName)
        .resources(V1ResourceRequirements().requests(mapOf("storage" to Quantity(jobmanagerStorageSize))))

    val jobmanagerStatefulSet = V1StatefulSet()
        .metadata(jobmanagerMetadata)
        .spec(V1StatefulSetSpec()
        .replicas(1)
        .template(jobmanagerTemplate)
        .updateStrategy(updateStrategy)
        .addVolumeClaimTemplatesItem(V1PersistentVolumeClaim().spec(jobmanagerVolumeClaim).metadata(V1ObjectMeta().name("jobmanager")))
        .serviceName("jobmanager")
        .selector(jobmanagerSelector))

    val jobmanagerStatefulSetOut = api.createNamespacedStatefulSet(NAMESPACE, jobmanagerStatefulSet, null, null, null)

    println("Job Manager StatefulSet created ${jobmanagerStatefulSetOut.metadata.name}")

    val taskmanager = V1Container()
        .image(flinkImageName)
        .imagePullPolicy("IfNotPresent")
        .name("flink-taskmanager")
        .command(listOf("/custom_entrypoint.sh"))
        .args(listOf("taskmanager"))
        .ports(listOf(port6121, port6122))
        .volumeMounts(listOf(taskmanagerVolumeMount))
        .env(listOf(podNameEnvVar, podNamespaceEnvVar, environmentEnvVar, rpcAddressEnvVar, flinkTaskManagerHeap, flinkTaskManagerNumberOfTaskSlots))
        .resources(taskmanagerResourceRequirements)

    val taskmanagerAffinity = V1Affinity()
        .podAntiAffinity(V1PodAntiAffinity()
            .preferredDuringSchedulingIgnoredDuringExecution(listOf(
                V1WeightedPodAffinityTerm().weight(50).podAffinityTerm(V1PodAffinityTerm().topologyKey("kubernetes.io/hostname").labelSelector(jobmanagerSelector)),
                V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(V1PodAffinityTerm().topologyKey("kubernetes.io/hostname").labelSelector(taskmanagerSelector))
            )
        )
    )

    val taskmanagerPodSpec = V1PodSpec().containers(listOf(taskmanager)).imagePullSecrets(listOf(pullSecretsReference)).affinity(taskmanagerAffinity)

    val taskmanagerMetadata = V1ObjectMeta().generateName("flink-taskmanager-").labels(mapOf(componentLabel, taskmanagerLabel))

    val taskmanagerTemplate = V1PodTemplateSpec().spec(taskmanagerPodSpec).metadata(taskmanagerMetadata)

    val taskmanagerVolumeClaim = V1PersistentVolumeClaimSpec()
        .accessModes(listOf("ReadWriteOnce"))
        .volumeName("flink")
        .storageClassName(storageClassName)
        .resources(V1ResourceRequirements().requests(mapOf("storage" to Quantity(taskmanagerStorageSize))))

    val taskmanagerStatefulSet = V1StatefulSet()
        .metadata(taskmanagerMetadata)
        .spec(V1StatefulSetSpec()
            .replicas(2)
            .template(taskmanagerTemplate)
            .updateStrategy(updateStrategy)
            .addVolumeClaimTemplatesItem(V1PersistentVolumeClaim().spec(taskmanagerVolumeClaim).metadata(V1ObjectMeta().name("taskmanager")))
            .serviceName("taskmanager")
            .selector(taskmanagerSelector))

    val taskmanagerStatefulSetOut = api.createNamespacedStatefulSet(NAMESPACE, taskmanagerStatefulSet, null, null, null)

    println("Task Manager StatefulSet created ${taskmanagerStatefulSetOut.metadata.name}")

    return jobmanagerServiceOut.metadata.name
}

private fun deleteFlinkCluster(name: String) {
    val api = AppsV1Api()

    val statefulSets = api.listNamespacedStatefulSet(NAMESPACE, null, null, null, null, "component=flink", 1, null, 30, null)

    statefulSets.items.forEach { statefulSet ->
        try {
            val status = api.deleteNamespacedStatefulSet(statefulSet.metadata.name, NAMESPACE, V1DeleteOptions(), "true", null, null, null, null)

            println("Response status: ${status.reason}")

            status.details.causes.forEach { println(it.message) }
        } catch (e : Exception) {
            // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
        }
    }

    val coreApi = CoreV1Api()

    val services = coreApi.listNamespacedService(NAMESPACE, null, null, null, null, "component=flink", 1, null, 30, null)

    services.items.forEach { service ->
        try {
            val status = coreApi.deleteNamespacedService(service.metadata.name, NAMESPACE, V1DeleteOptions(), "true", null, null, null, null)

            println("Response status: ${status.reason}")

            status.details.causes.forEach { println(it.message) }
        } catch (e : Exception) {
            // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
        }
    }
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

    val list = api.listNamespacedPod(NAMESPACE, null, null, null, null, null, null, null, 10, null)

    list.items.forEach {
        item -> println("Pod name: ${item.metadata.name}")
    }

    list.items.forEach {
        item -> println("${item.status}")
    }
}
