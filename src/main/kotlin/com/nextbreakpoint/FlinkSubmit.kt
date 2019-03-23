package com.nextbreakpoint

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.google.common.io.ByteStreams.copy
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

class FlinkSubmit : CliktCommand() {
    private val NAMESPACE = "default"

    private val kubeConfig: String by option(help="The path of Kubectl config").required()
    private val dockerImage: String by option(help="The Docker image with Flink job").required()
    private val clusterName: String by option(help="The name of the Flink cluster").required()

    override fun run() {
        try {
            run(kubeConfig, dockerImage, clusterName)
        } catch (e : Exception) {
            e.printStackTrace()
        } finally {
            System.exit(0)
        }
    }

    private fun run(kubeConfigPath: String, dockerImage: String, clusterName: String) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

        Configuration.setDefaultApiClient(client)

        createCluster(clusterName, dockerImage)

        Thread.sleep(60000)

        listClusterPods(clusterName)

        submitJob(clusterName)

        Thread.sleep(60000)

        listJobs(clusterName)

        Thread.sleep(60000)

        deleteCluster(clusterName)
    }

    private fun createCluster(clusterName: String, dockerImage: String) {
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

        val clusterLabel = Pair("cluster", clusterName)

        val jobmanagerLabel = Pair("role", "jobmanager")

        val taskmanagerLabel = Pair("role", "taskmanager")

        val jobmanagerResourceRequirements = V1ResourceRequirements()
            .limits(
                mapOf(
                    "cpu" to Quantity("1"),
                    "memory" to Quantity("600Mi")
                )
            )
            .requests(
                mapOf(
                    "cpu" to Quantity("0.2"),
                    "memory" to Quantity("200Mi")
                )
            )

        val taskmanagerResourceRequirements = V1ResourceRequirements()
            .limits(
                mapOf(
                    "cpu" to Quantity("1"),
                    "memory" to Quantity("1200Mi")
                )
            )
            .requests(
                mapOf(
                    "cpu" to Quantity("0.2"),
                    "memory" to Quantity("500Mi")
                )
            )

        val jobmanagerSelector = V1LabelSelector().matchLabels(
            mapOf(
                clusterLabel,
                componentLabel,
                jobmanagerLabel
            )
        )

        val taskmanagerSelector = V1LabelSelector().matchLabels(
            mapOf(
                clusterLabel,
                componentLabel,
                taskmanagerLabel
            )
        )

        val environmentEnvVar = V1EnvVar()
            .name("FLINK_ENVIRONMENT")
            .value(environment)

        val flinkJobManagerHeap = V1EnvVar()
            .name("FLINK_JM_HEAP")
            .value("512")

        val flinkTaskManagerHeap = V1EnvVar()
            .name("FLINK_TM_HEAP")
            .value("1024")

        val flinkTaskManagerNumberOfTaskSlots = V1EnvVar()
            .name("TASK_MANAGER_NUMBER_OF_TASK_SLOTS")
            .value("1")

        val podNameEnvVar = V1EnvVar()
            .name("POD_NAME")
            .valueFrom(
                V1EnvVarSource()
                    .fieldRef(
                        V1ObjectFieldSelector().fieldPath("metadata.name")
                    )
            )

        val podNamespaceEnvVar = V1EnvVar()
            .name("POD_NAMESPACE")
            .valueFrom(
                V1EnvVarSource()
                    .fieldRef(
                        V1ObjectFieldSelector().fieldPath("metadata.namespace")
                    )
            )

        val jobmanagerVolumeMount = V1VolumeMount()
            .mountPath("/var/tmp/data")
            .subPath("data")
            .name("jobmanager")

        val taskmanagerVolumeMount = V1VolumeMount()
            .mountPath("/var/tmp/data")
            .subPath("data")
            .name("taskmanager")

        val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

        val jobmanagerServiceSpec = V1ServiceSpec()
            .ports(
                listOf(
                    srvPort8081,
                    srvPort6123,
                    srvPort6124,
                    srvPort6125
                )
            )
            .selector(
                mapOf(
                    clusterLabel,
                    componentLabel,
                    jobmanagerLabel
                )
            )
            .type("NodePort")

        val jobmanagerServiceMetadata = V1ObjectMeta()
            .generateName("flink-jobmanager-")
            .labels(
                mapOf(
                    clusterLabel,
                    componentLabel,
                    jobmanagerLabel
                )
            )

        val jobmanagerService = V1Service()
            .spec(jobmanagerServiceSpec)
            .metadata(jobmanagerServiceMetadata)

        println("Creating Flink Service ...")

        val jobmanagerServiceOut = coreApi.createNamespacedService(NAMESPACE, jobmanagerService, null, null, null)

        println("Service created ${jobmanagerServiceOut.metadata.name}")

        val rpcAddressEnvVar = V1EnvVar()
            .name("JOB_MANAGER_RPC_ADDRESS")
            .value(jobmanagerServiceOut.metadata.name)

        val jobmanager = V1Container()
            .image(dockerImage)
            .imagePullPolicy("IfNotPresent")
            .name("flink-jobmanager")
            .command(
                listOf("/custom_entrypoint.sh")
            )
            .args(
                listOf("jobmanager")
            )
            .ports(
                listOf(
                    port8081,
                    port6123,
                    port6124,
                    port6125
                )
            )
            .volumeMounts(listOf(jobmanagerVolumeMount))
            .env(
                listOf(
                    podNameEnvVar,
                    podNamespaceEnvVar,
                    environmentEnvVar,
                    rpcAddressEnvVar,
                    flinkJobManagerHeap
                )
            )
            .resources(jobmanagerResourceRequirements)

//    val jobmanagerNodeSelector = V1NodeSelector().nodeSelectorTerms(listOf(
//        V1NodeSelectorTerm().matchExpressions(listOf(
//            V1NodeSelectorRequirement().key("kubernetes.io/hostname").operator("in").values(listOf("k8s1")))
//        )
//    ))

        val jobmanagerAffinity = V1Affinity()
//        .nodeAffinity(V1NodeAffinity().requiredDuringSchedulingIgnoredDuringExecution(jobmanagerNodeSelector))
            .podAntiAffinity(
                V1PodAntiAffinity().preferredDuringSchedulingIgnoredDuringExecution(
                    listOf(
                        V1WeightedPodAffinityTerm().weight(50).podAffinityTerm(
                            V1PodAffinityTerm()
                                .topologyKey("kubernetes.io/hostname")
                                .labelSelector(jobmanagerSelector)
                        ),
                        V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                            V1PodAffinityTerm()
                                .topologyKey("kubernetes.io/hostname")
                                .labelSelector(taskmanagerSelector)
                        )
                    )
                )
            )

        val jobmanagerPodSpec = V1PodSpec()
            .containers(
                listOf(jobmanager)
            )
            .imagePullSecrets(
                listOf(pullSecretsReference)
            )
            .affinity(jobmanagerAffinity)

        val jobmanagerMetadata = V1ObjectMeta()
            .generateName("flink-jobmanager-")
            .labels(
                mapOf(
                    clusterLabel,
                    componentLabel,
                    jobmanagerLabel
                )
            )

        val jobmanagerVolumeClaim = V1PersistentVolumeClaimSpec()
            .accessModes(listOf("ReadWriteOnce"))
            .storageClassName(storageClassName)
            .resources(
                V1ResourceRequirements()
                    .requests(
                        mapOf("storage" to Quantity(jobmanagerStorageSize))
                    )
            )

        val jobmanagerStatefulSet = V1StatefulSet()
            .metadata(
                V1ObjectMeta()
                    .generateName("flink-jobmanager-")
                    .labels(
                        mapOf(
                            clusterLabel,
                            componentLabel,
                            jobmanagerLabel
                        )
                    )
            )
            .spec(
                V1StatefulSetSpec()
                    .replicas(1)
                    .template(
                        V1PodTemplateSpec()
                            .spec(jobmanagerPodSpec)
                            .metadata(jobmanagerMetadata)
                    )
                    .updateStrategy(updateStrategy)
                    .serviceName("jobmanager")
                    .selector(jobmanagerSelector)
                    .addVolumeClaimTemplatesItem(
                        V1PersistentVolumeClaim()
                            .spec(jobmanagerVolumeClaim)
                            .metadata(
                                V1ObjectMeta()
                                    .name("jobmanager")
                                    .labels(
                                        mapOf(
                                            clusterLabel,
                                            componentLabel,
                                            jobmanagerLabel
                                        )
                                    )
                            )
                    )
            )

        println("Creating JobManager StatefulSet ...")

        val jobmanagerStatefulSetOut = api.createNamespacedStatefulSet(NAMESPACE, jobmanagerStatefulSet, null, null, null)

        println("StatefulSet created ${jobmanagerStatefulSetOut.metadata.name}")

        val taskmanager = V1Container()
            .image(dockerImage)
            .imagePullPolicy("IfNotPresent")
            .name("flink-taskmanager")
            .command(
                listOf("/custom_entrypoint.sh")
            )
            .args(
                listOf("taskmanager")
            )
            .ports(
                listOf(
                    port6121,
                    port6122
                )
            )
            .volumeMounts(
                listOf(taskmanagerVolumeMount)
            )
            .env(
                listOf(
                    podNameEnvVar,
                    podNamespaceEnvVar,
                    environmentEnvVar,
                    rpcAddressEnvVar,
                    flinkTaskManagerHeap,
                    flinkTaskManagerNumberOfTaskSlots
                )
            )
            .resources(taskmanagerResourceRequirements)

        val taskmanagerAffinity = V1Affinity()
            .podAntiAffinity(V1PodAntiAffinity()
                .preferredDuringSchedulingIgnoredDuringExecution(
                    listOf(
                        V1WeightedPodAffinityTerm().weight(50).podAffinityTerm(
                            V1PodAffinityTerm()
                                .topologyKey("kubernetes.io/hostname")
                                .labelSelector(jobmanagerSelector)
                        ),
                        V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                            V1PodAffinityTerm()
                                .topologyKey("kubernetes.io/hostname")
                                .labelSelector(taskmanagerSelector)
                        )
                    )
                )
            )

        val taskmanagerPodSpec = V1PodSpec()
            .containers(
                listOf(taskmanager)
            )
            .imagePullSecrets(
                listOf(pullSecretsReference)
            )
            .affinity(taskmanagerAffinity)

        val taskmanagerMetadata = V1ObjectMeta()
            .generateName("flink-taskmanager-")
            .labels(
                mapOf(
                    clusterLabel,
                    componentLabel,
                    taskmanagerLabel
                )
            )

        val taskmanagerVolumeClaim = V1PersistentVolumeClaimSpec()
            .accessModes(listOf("ReadWriteOnce"))
            .storageClassName(storageClassName)
            .resources(
                V1ResourceRequirements()
                    .requests(
                        mapOf("storage" to Quantity(taskmanagerStorageSize))
                    )
            )

        val taskmanagerStatefulSet = V1StatefulSet()
            .metadata(taskmanagerMetadata)
            .spec(
                V1StatefulSetSpec()
                    .replicas(2)
                    .template(
                        V1PodTemplateSpec()
                            .spec(taskmanagerPodSpec)
                            .metadata(taskmanagerMetadata)
                    )
                    .updateStrategy(updateStrategy)
                    .serviceName("taskmanager")
                    .selector(taskmanagerSelector)
                    .addVolumeClaimTemplatesItem(
                        V1PersistentVolumeClaim()
                            .spec(taskmanagerVolumeClaim)
                            .metadata(
                                V1ObjectMeta()
                                    .name("taskmanager")
                                    .labels(
                                        mapOf(
                                            clusterLabel,
                                            componentLabel,
                                            taskmanagerLabel
                                        )
                                    )
                            )
                    )
            )

        println("Creating TaskManager StatefulSet ...")

        val taskmanagerStatefulSetOut = api.createNamespacedStatefulSet(NAMESPACE, taskmanagerStatefulSet, null, null, null)

        println("StatefulSet created ${taskmanagerStatefulSetOut.metadata.name}")
    }

    private fun deleteCluster(clusterName: String) {
        println("Deleting cluster $clusterName...")

        val api = AppsV1Api()

        val statefulSets = api.listNamespacedStatefulSet(NAMESPACE, null, null, null, null, "cluster=$clusterName", null, null, 30, null)

        statefulSets.items.forEach { statefulSet ->
            try {
                println("Removing StatefulSet ${statefulSet.metadata.name}...")

                val status = api.deleteNamespacedStatefulSet(statefulSet.metadata.name, NAMESPACE, V1DeleteOptions(), "true", null, null, null, null)

                println("Response status: ${status.reason}")

                status.details.causes.forEach { println(it.message) }
            } catch (e : Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }

        val coreApi = CoreV1Api()

        val services = coreApi.listNamespacedService(NAMESPACE, null, null, null, null, "cluster=$clusterName", null, null, 30, null)

        services.items.forEach { service ->
            try {
                println("Removing Service ${service.metadata.name}...")

                val status = coreApi.deleteNamespacedService(service.metadata.name, NAMESPACE, V1DeleteOptions(), "true", null, null, null, null)

                println("Response status: ${status.reason}")

                status.details.causes.forEach { println(it.message) }
            } catch (e : Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }

        val volumeClaims = coreApi.listNamespacedPersistentVolumeClaim(NAMESPACE, null, null, null, null, "cluster=$clusterName", null, null, 30, null)

        volumeClaims.items.forEach { volumeClaim ->
            try {
                println("Removing Persistent Volume Claim ${volumeClaim.metadata.name}...")

                val status = coreApi.deleteNamespacedPersistentVolumeClaim(volumeClaim.metadata.name, NAMESPACE, V1DeleteOptions(), "true", null, null, null, null)

                println("Response status: ${status.reason}")

                status.details.causes.forEach { println(it.message) }
            } catch (e : Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }

        println("Done.")
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

    private fun listJobs(clusterName: String) {
        val coreApi = CoreV1Api()

        val exec = Exec()

//    val services = coreApi.listNamespacedService(NAMESPACE, null, null, null, "metadata.name=$serviceName", "role=jobmanager", 1, null, 30, null)
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

    private fun createServicePort(port: Int, name: String) = V1ServicePort()
        .protocol("TCP")
        .port(port)
        .targetPort(IntOrString(name))
        .name(name)

    private fun createContainerPort(port: Int, name: String) = V1ContainerPort()
        .protocol("TCP")
        .containerPort(port)
        .name(name)

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

