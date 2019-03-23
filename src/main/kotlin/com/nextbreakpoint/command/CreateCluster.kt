package com.nextbreakpoint.command

import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.custom.IntOrString
import io.kubernetes.client.models.*
import io.kubernetes.client.util.Config
import java.io.File
import java.io.FileInputStream
import io.kubernetes.client.custom.Quantity

class CreateCluster {
    private val NAMESPACE = "default"

    fun run(kubeConfigPath: String, dockerImage: String, clusterName: String) {
        val client = Config.fromConfig(FileInputStream(File(kubeConfigPath)))

        Configuration.setDefaultApiClient(client)

        createCluster(clusterName, dockerImage)
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

    private fun createServicePort(port: Int, name: String) = V1ServicePort()
        .protocol("TCP")
        .port(port)
        .targetPort(IntOrString(name))
        .name(name)

    private fun createContainerPort(port: Int, name: String) = V1ContainerPort()
        .protocol("TCP")
        .containerPort(port)
        .name(name)
}

