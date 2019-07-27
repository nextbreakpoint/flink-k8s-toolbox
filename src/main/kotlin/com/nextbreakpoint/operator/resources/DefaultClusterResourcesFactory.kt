package com.nextbreakpoint.operator.resources

import com.nextbreakpoint.model.V1FlinkCluster
import io.kubernetes.client.custom.IntOrString
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.V1Affinity
import io.kubernetes.client.models.V1Container
import io.kubernetes.client.models.V1ContainerBuilder
import io.kubernetes.client.models.V1ContainerPort
import io.kubernetes.client.models.V1EnvVar
import io.kubernetes.client.models.V1EnvVarSource
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1JobBuilder
import io.kubernetes.client.models.V1LabelSelector
import io.kubernetes.client.models.V1LocalObjectReference
import io.kubernetes.client.models.V1ObjectFieldSelector
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1PersistentVolumeClaimSpec
import io.kubernetes.client.models.V1PodAffinity
import io.kubernetes.client.models.V1PodAffinityTerm
import io.kubernetes.client.models.V1PodAntiAffinity
import io.kubernetes.client.models.V1PodSpecBuilder
import io.kubernetes.client.models.V1ResourceRequirements
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1ServiceBuilder
import io.kubernetes.client.models.V1ServicePort
import io.kubernetes.client.models.V1StatefulSet
import io.kubernetes.client.models.V1StatefulSetBuilder
import io.kubernetes.client.models.V1StatefulSetUpdateStrategy
import io.kubernetes.client.models.V1VolumeMount
import io.kubernetes.client.models.V1WeightedPodAffinityTerm

object DefaultClusterResourcesFactory : ClusterResourcesFactory {
    override fun createJobManagerService(
        namespace: String,
        clusterId: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1Service {
        if (flinkCluster.metadata.name == null) {
            throw RuntimeException("name is required")
        }

        val serviceLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("name", flinkCluster.metadata.name),
            Pair("uid", clusterId),
            Pair("component", "flink"),
            Pair("role", "jobmanager")
        )

        val srvPort8081 = createServicePort(8081, "ui")
        val srvPort6123 = createServicePort(6123, "rpc")
        val srvPort6124 = createServicePort(6124, "blob")
        val srvPort6125 = createServicePort(6125, "query")

        return V1ServiceBuilder()
            .editOrNewMetadata()
            .withName("flink-jobmanager-${flinkCluster.metadata.name}")
            .withLabels(serviceLabels)
            .endMetadata()
            .editOrNewSpec()
            .addToPorts(srvPort8081)
            .addToPorts(srvPort6123)
            .addToPorts(srvPort6124)
            .addToPorts(srvPort6125)
            .withSelector(serviceLabels)
            .withType(flinkCluster.spec.jobManagerSpec?.serviceMode ?: "ClusterIP")
            .endSpec()
            .build()
    }

    override fun createJarUploadJob(
        namespace: String,
        clusterId: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1Job {
        if (flinkCluster.metadata.name == null) {
            throw RuntimeException("name is required")
        }

        if (flinkCluster.spec.flinkJobSpec == null) {
            throw RuntimeException("flinkJobSpec is required")
        }

        if (flinkCluster.spec.flinkJobSpec.image == null) {
            throw RuntimeException("image is required")
        }

        if (flinkCluster.spec.flinkJobSpec.jarPath == null) {
            throw RuntimeException("jarPath is required")
        }

        val jobLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("name", flinkCluster.metadata.name),
            Pair("uid", clusterId),
            Pair("component", "flink")
        )

        val podNameEnvVar = createEnvVarFromField(
            "POD_NAME", "metadata.name"
        )

        val podNamespaceEnvVar = createEnvVarFromField(
            "POD_NAMESPACE", "metadata.namespace"
        )

        val arguments = createJarUploadArguments(
            namespace, flinkCluster.metadata.name, flinkCluster.spec.flinkJobSpec.jarPath
        )

        val jobSelector = V1LabelSelector().matchLabels(jobLabels)

        val jobAffinity = createUploadJobAffinity(jobSelector)

        val pullSecrets = createObjectReferenceListOrNull(flinkCluster.spec.pullSecrets)

        val jobPodSpec = V1PodSpecBuilder()
            .addToContainers(V1Container())
            .editFirstContainer()
            .withName("flink-upload")
            .withImage(flinkCluster.spec.flinkJobSpec.image)
            .withImagePullPolicy(flinkCluster.spec.pullPolicy ?: "Always")
            .withArgs(arguments)
            .addToEnv(podNameEnvVar)
            .addToEnv(podNamespaceEnvVar)
            .withResources(createUploadJobResourceRequirements())
            .endContainer()
            .withServiceAccountName("flink-upload"/* TODO make configurable */)
            .withImagePullSecrets(pullSecrets)
            .withRestartPolicy("OnFailure")
            .withAffinity(jobAffinity)
            .build()

        val job = V1JobBuilder()
            .editOrNewMetadata()
            .withName("flink-upload-${flinkCluster.metadata.name}")
            .withLabels(jobLabels)
            .endMetadata()
            .editOrNewSpec()
            .withCompletions(1)
            .withParallelism(1)
            .withBackoffLimit(3)
            .withNewTtlSecondsAfterFinished(30)
            .editOrNewTemplate()
            .editOrNewMetadata()
            .withName("flink-upload-${flinkCluster.metadata.name}")
            .withLabels(jobLabels)
            .endMetadata()
            .withSpec(jobPodSpec)
            .endTemplate()
            .endSpec()
            .build()

        return job
    }

    override fun createJobManagerStatefulSet(
        namespace: String,
        clusterId: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1StatefulSet {
        if (flinkCluster.metadata.name == null) {
            throw RuntimeException("name is required")
        }

        if (flinkCluster.spec.flinkImage == null) {
            throw RuntimeException("flinkImage is required")
        }

        val jobmanagerLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("name", flinkCluster.metadata.name),
            Pair("uid", clusterId),
            Pair("component", "flink"),
            Pair("role", "jobmanager")
        )

        val taskmanagerLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("name", flinkCluster.metadata.name),
            Pair("uid", clusterId),
            Pair("component", "flink"),
            Pair("role", "taskmanager")
        )

        val jobmanagerVolumeMount = createVolumeMount("jobmanager")

        val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

        val port8081 = createContainerPort(8081, "ui")
        val port6123 = createContainerPort(6123, "rpc")
        val port6124 = createContainerPort(6124, "blob")
        val port6125 = createContainerPort(6125, "query")

        val podNameEnvVar = createEnvVarFromField(
            "POD_NAME", "metadata.name"
        )

        val podNamespaceEnvVar = createEnvVarFromField(
            "POD_NAMESPACE", "metadata.namespace"
        )

        val jobManagerHeapEnvVar = createEnvVar(
            "FLINK_JM_HEAP", flinkCluster.spec.jobManagerSpec?.requiredMemory?.toString() ?: "256"
        )

        val rpcAddressEnvVar = createEnvVar(
            "JOB_MANAGER_RPC_ADDRESS", "flink-jobmanager-${flinkCluster.metadata.name}"
        )

        val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

        val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

        val jobmanagerAffinity = createAffinity(
            jobmanagerSelector, taskmanagerSelector
        )

        val jobmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            jobManagerHeapEnvVar,
            rpcAddressEnvVar
        )

        if (flinkCluster.spec.jobManagerSpec?.environment != null) {
            jobmanagerVariables.addAll(flinkCluster.spec.jobManagerSpec.environment)
        }

        val jobmanagerContainer = V1ContainerBuilder()
            .withImage(flinkCluster.spec.flinkImage)
            .withImagePullPolicy(flinkCluster.spec.pullPolicy ?: "Always")
            .withName("flink-jobmanager")
            .withArgs(listOf("jobmanager"))
            .addToPorts(port8081)
            .addToPorts(port6123)
            .addToPorts(port6124)
            .addToPorts(port6125)
            .addToVolumeMounts(jobmanagerVolumeMount)
            .withEnv(jobmanagerVariables)
            .withResources(
                createResourceRequirements(
                    flinkCluster.spec.jobManagerSpec?.requiredCPUs ?: 1.0f,
                    flinkCluster.spec.jobManagerSpec?.requiredMemory ?: 256
                )
            )
            .build()

        val jobmanagerPullSecrets = if (flinkCluster.spec.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(flinkCluster.spec.pullSecrets)
            )
        } else null

        val jobmanagerPodSpec = V1PodSpecBuilder()
            .addToContainers(jobmanagerContainer)
            .withServiceAccountName(flinkCluster.spec.serviceAccount ?: "default")
            .withImagePullSecrets(jobmanagerPullSecrets)
            .withAffinity(jobmanagerAffinity)
            .build()

        val jobmanagerMetadata = createObjectMeta(
            "flink-jobmanager-${flinkCluster.metadata.name}", jobmanagerLabels
        )

        val jobmanagerVolumeClaim = createPersistentVolumeClaimSpec(
            flinkCluster.spec.jobManagerSpec?.storageClass ?: "standard",
            flinkCluster.spec.jobManagerSpec?.requiredStorageSize ?: 1
        )

        return V1StatefulSetBuilder()
            .withMetadata(jobmanagerMetadata)
            .editOrNewSpec()
            .withReplicas(1)
            .editOrNewTemplate()
            .withSpec(jobmanagerPodSpec)
            .withMetadata(jobmanagerMetadata)
            .endTemplate()
            .withUpdateStrategy(updateStrategy)
            .withServiceName("jobmanager")
            .withSelector(jobmanagerSelector)
            .addNewVolumeClaimTemplate()
            .withSpec(jobmanagerVolumeClaim)
            .editOrNewMetadata()
            .withName("jobmanager")
            .withLabels(jobmanagerLabels)
            .endMetadata()
            .endVolumeClaimTemplate()
            .endSpec()
            .build()
    }

    override fun createTaskManagerStatefulSet(
        namespace: String,
        clusterId: String,
        clusterOwner: String,
        flinkCluster: V1FlinkCluster
    ): V1StatefulSet {
        if (flinkCluster.metadata.name == null) {
            throw RuntimeException("name is required")
        }

        if (flinkCluster.spec.flinkImage == null) {
            throw RuntimeException("flinkImage is required")
        }

        val jobmanagerLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("name", flinkCluster.metadata.name),
            Pair("uid", clusterId),
            Pair("component", "flink"),
            Pair("role", "jobmanager")
        )

        val taskmanagerLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("name", flinkCluster.metadata.name),
            Pair("uid", clusterId),
            Pair("component", "flink"),
            Pair("role", "taskmanager")
        )

        val taskmanagerVolumeMount = createVolumeMount("taskmanager")

        val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

        val port6121 = createContainerPort(6121, "data")
        val port6122 = createContainerPort(6122, "ipc")

        val podNameEnvVar = createEnvVarFromField(
            "POD_NAME", "metadata.name"
        )

        val podNamespaceEnvVar = createEnvVarFromField(
            "POD_NAMESPACE", "metadata.namespace"
        )

        val taskManagerHeapEnvVar = createEnvVar(
            "FLINK_TM_HEAP", flinkCluster.spec.taskManagerSpec?.requiredMemory?.toString() ?: "1024"
        )

        val rpcAddressEnvVar = createEnvVar(
            "JOB_MANAGER_RPC_ADDRESS", "flink-jobmanager-${flinkCluster.metadata.name}"
        )

        val numberOfTaskSlotsEnvVar = createEnvVar(
            "TASK_MANAGER_NUMBER_OF_TASK_SLOTS", flinkCluster.spec.taskManagerSpec?.taskSlots?.toString() ?: "1"
        )

        val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

        val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

        val taskmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            taskManagerHeapEnvVar,
            rpcAddressEnvVar,
            numberOfTaskSlotsEnvVar
        )

        if (flinkCluster.spec.taskManagerSpec?.environment != null) {
            taskmanagerVariables.addAll(flinkCluster.spec.taskManagerSpec.environment)
        }

        val taskmanagerContainer = V1ContainerBuilder()
            .withImage(flinkCluster.spec.flinkImage)
            .withImagePullPolicy(flinkCluster.spec.pullPolicy ?: "Always")
            .withName("flink-taskmanager")
            .withArgs(listOf("taskmanager"))
            .addToPorts(port6121)
            .addToPorts(port6122)
            .addToVolumeMounts(taskmanagerVolumeMount)
            .withEnv(taskmanagerVariables)
            .withResources(
                createResourceRequirements(
                    flinkCluster.spec.taskManagerSpec?.requiredCPUs ?: 1.0f,
                    flinkCluster.spec.taskManagerSpec?.requiredMemory ?: 1024
                )
            )
            .build()

        val taskmanagerAffinity = createAffinity(
            jobmanagerSelector, taskmanagerSelector
        )

        val taskmanagerPullSecrets = if (flinkCluster.spec.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(flinkCluster.spec.pullSecrets)
            )
        } else null

        val taskmanagerPodSpec = V1PodSpecBuilder()
            .addToContainers(taskmanagerContainer)
            .withServiceAccountName(flinkCluster.spec.serviceAccount ?: "default")
            .withImagePullSecrets(taskmanagerPullSecrets)
            .withAffinity(taskmanagerAffinity)
            .build()

        val taskmanagerMetadata = createObjectMeta(
            "flink-taskmanager-${flinkCluster.metadata.name}", taskmanagerLabels
        )

        val taskmanagerVolumeClaim = createPersistentVolumeClaimSpec(
            flinkCluster.spec.taskManagerSpec?.storageClass ?: "standard",
            flinkCluster.spec.taskManagerSpec?.requiredStorageSize ?: 5
        )

        return V1StatefulSetBuilder()
            .withMetadata(taskmanagerMetadata)
            .editOrNewSpec()
            .withReplicas(flinkCluster.spec.taskManagerSpec?.replicas ?: 1)
            .editOrNewTemplate()
            .withSpec(taskmanagerPodSpec)
            .withMetadata(taskmanagerMetadata)
            .endTemplate()
            .withUpdateStrategy(updateStrategy)
            .withServiceName("taskmanager")
            .withSelector(taskmanagerSelector)
            .addNewVolumeClaimTemplate()
            .withSpec(taskmanagerVolumeClaim)
            .editOrNewMetadata()
            .withName("taskmanager")
            .withLabels(taskmanagerLabels)
            .endMetadata()
            .endVolumeClaimTemplate()
            .endSpec()
            .build()
    }

    private fun createAffinity(
        jobmanagerSelector: V1LabelSelector?,
        taskmanagerSelector: V1LabelSelector?
    ): V1Affinity = V1Affinity()
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

    private fun createUploadJobAffinity(
        jobSelector: V1LabelSelector?
    ): V1Affinity = V1Affinity()
        .podAffinity(
            V1PodAffinity().preferredDuringSchedulingIgnoredDuringExecution(
                listOf(
                    V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                        V1PodAffinityTerm()
                            .topologyKey("kubernetes.io/hostname")
                            .labelSelector(jobSelector)
                    )
                )
            )
        )

    private fun createPersistentVolumeClaimSpec(
        storageClass: String,
        storageSize: Int
    ) = V1PersistentVolumeClaimSpec()
        .accessModes(listOf("ReadWriteOnce"))
        .storageClassName(storageClass)
        .resources(
            V1ResourceRequirements()
                .requests(
                    mapOf("storage" to Quantity(storageSize.toString()))
                )
        )

    private fun createObjectMeta(name: String, labels: Map<String, String>) = V1ObjectMeta().name(name).labels(labels)

    private fun createVolumeMount(name: String) = V1VolumeMount().mountPath("/var/tmp").subPath(name).name(name)

    private fun createEnvVarFromField(name: String, fieldPath: String) =
        V1EnvVar().name(name).valueFrom(
            V1EnvVarSource().fieldRef(V1ObjectFieldSelector().fieldPath(fieldPath))
        )

    private fun createEnvVar(name: String, value: String) = V1EnvVar().name(name).value(value)

    private fun createResourceRequirements(cpus: Float, memory: Int) = V1ResourceRequirements()
        .limits(
            mapOf(
                "cpu" to Quantity(cpus.toString()),
                "memory" to Quantity(memory.times(1.5).toString() + "Mi")
            )
        )
        .requests(
            mapOf(
                "cpu" to Quantity(cpus.div(4).toString()),
                "memory" to Quantity(memory.toString() + "Mi")
            )
        )

    private fun createUploadJobResourceRequirements() = V1ResourceRequirements()
        .limits(
            mapOf(
                "cpu" to Quantity("0.2"),
                "memory" to Quantity("200Mi")
            )
        )
        .requests(
            mapOf(
                "cpu" to Quantity("0.2"),
                "memory" to Quantity("200Mi")
            )
        )

    private fun createServicePort(port: Int, name: String) = V1ServicePort()
        .protocol("TCP")
        .port(port)
        .targetPort(IntOrString(name))
        .name(name)

    private fun createContainerPort(port: Int, name: String) = V1ContainerPort()
        .protocol("TCP")
        .containerPort(port)
        .name(name)

    private fun createObjectReferenceListOrNull(referenceName: String?): List<V1LocalObjectReference>? {
        return if (referenceName != null) {
            listOf(
                V1LocalObjectReference().name(referenceName)
            )
        } else null
    }

    private fun createJarUploadArguments(
        namespace: String,
        clusterName: String,
        jarPath: String
    ): List<String> {
        val arguments = mutableListOf<String>()

        arguments.addAll(
            listOf(
                "upload",
                "jar",
                "--namespace=$namespace",
                "--cluster-name=$clusterName",
                "--jar-path=$jarPath"
            )
        )

        return arguments.toList()
    }
}