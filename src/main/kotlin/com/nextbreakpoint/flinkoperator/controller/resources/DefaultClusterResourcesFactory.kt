package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import io.kubernetes.client.custom.IntOrString
import io.kubernetes.client.models.V1Affinity
import io.kubernetes.client.models.V1ContainerBuilder
import io.kubernetes.client.models.V1ContainerPort
import io.kubernetes.client.models.V1EnvVar
import io.kubernetes.client.models.V1EnvVarSource
import io.kubernetes.client.models.V1HTTPGetAction
import io.kubernetes.client.models.V1LabelSelector
import io.kubernetes.client.models.V1LocalObjectReference
import io.kubernetes.client.models.V1ObjectFieldSelector
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1PodAffinityTerm
import io.kubernetes.client.models.V1PodAntiAffinity
import io.kubernetes.client.models.V1PodSpecBuilder
import io.kubernetes.client.models.V1Probe
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1ServiceBuilder
import io.kubernetes.client.models.V1ServicePort
import io.kubernetes.client.models.V1StatefulSet
import io.kubernetes.client.models.V1StatefulSetBuilder
import io.kubernetes.client.models.V1StatefulSetUpdateStrategy
import io.kubernetes.client.models.V1TCPSocketAction
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

        val srvPort8081 =
            createServicePort(
                8081,
                "ui"
            )
        val srvPort6123 =
            createServicePort(
                6123,
                "rpc"
            )
        val srvPort6124 =
            createServicePort(
                6124,
                "blob"
            )
        val srvPort6125 =
            createServicePort(
                6125,
                "query"
            )

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
            .withType(flinkCluster.spec.jobManager?.serviceMode ?: "ClusterIP")
            .endSpec()
            .build()
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

        if (flinkCluster.spec.runtime?.image == null) {
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

        val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

        val port8081 =
            createContainerPort(
                8081,
                "ui"
            )
        val port6123 =
            createContainerPort(
                6123,
                "rpc"
            )
        val port6124 =
            createContainerPort(
                6124,
                "blob"
            )
        val port6125 =
            createContainerPort(
                6125,
                "query"
            )

        val podNameEnvVar =
            createEnvVarFromField(
                "POD_NAME", "metadata.name"
            )

        val podNamespaceEnvVar =
            createEnvVarFromField(
                "POD_NAMESPACE", "metadata.namespace"
            )

        val rpcAddressEnvVar =
            createEnvVar(
                "JOB_MANAGER_RPC_ADDRESS", "flink-jobmanager-${flinkCluster.metadata.name}"
            )

        val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

        val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

        val jobmanagerAffinity =
            createAffinity(
                jobmanagerSelector, taskmanagerSelector
            )

        val jobmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            rpcAddressEnvVar
        )

        if (flinkCluster.spec.jobManager?.environment != null) {
            jobmanagerVariables.addAll(flinkCluster.spec.jobManager.environment)
        }

        val jobmanagerContainer = V1ContainerBuilder()
            .withImage(flinkCluster.spec.runtime?.image)
            .withImagePullPolicy(flinkCluster.spec.runtime?.pullPolicy ?: "Always")
            .withName("flink-jobmanager")
            .withArgs(listOf("jobmanager"))
            .addToPorts(port8081)
            .addToPorts(port6123)
            .addToPorts(port6124)
            .addToPorts(port6125)
            .addAllToPorts(flinkCluster.spec.jobManager?.extraPorts ?: listOf())
            .addAllToVolumeMounts(flinkCluster.spec.jobManager?.volumeMounts ?: listOf())
            .withEnv(jobmanagerVariables)
            .withEnvFrom(flinkCluster.spec.jobManager?.environmentFrom)
            .withResources(flinkCluster.spec.jobManager?.resources)
            .withLivenessProbe(
                V1Probe()
                    .httpGet(V1HTTPGetAction().port(IntOrString(8081)).path("/overview"))
                    .initialDelaySeconds(30)
                    .periodSeconds(10)
            )
            .withLivenessProbe(
                V1Probe()
                    .tcpSocket(V1TCPSocketAction().port(IntOrString(6123)))
                    .initialDelaySeconds(30)
                    .periodSeconds(10)
            )
            .withReadinessProbe(
                V1Probe()
                    .httpGet(V1HTTPGetAction().port(IntOrString(8081)).path("/overview"))
                    .initialDelaySeconds(15)
                    .periodSeconds(5)
            )
            .withReadinessProbe(
                V1Probe()
                    .tcpSocket(V1TCPSocketAction().port(IntOrString(6123)))
                    .initialDelaySeconds(15)
                    .periodSeconds(5)
            )
            .build()

        val jobmanagerPullSecrets = if (flinkCluster.spec.runtime?.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(flinkCluster.spec.runtime?.pullSecrets)
            )
        } else null

        val initContainers = flinkCluster.spec.jobManager?.initContainers ?: listOf()

        val sideContainers = flinkCluster.spec.jobManager?.sideContainers ?: listOf()

        val jobmanagerPodSpec = V1PodSpecBuilder()
            .addAllToInitContainers(initContainers)
            .addToContainers(jobmanagerContainer)
            .addAllToContainers(sideContainers)
            .withServiceAccountName(flinkCluster.spec.jobManager?.serviceAccount ?: "default")
            .withImagePullSecrets(jobmanagerPullSecrets)
            .withAffinity(jobmanagerAffinity)
            .withVolumes(flinkCluster.spec.jobManager?.volumes)
            .build()

        val jobmanagerMetadata =
            createObjectMeta(
                "flink-jobmanager-${flinkCluster.metadata.name}", jobmanagerLabels
            )

        val jobmanagerPodMetadata =
            createObjectMeta(
                "flink-jobmanager-${flinkCluster.metadata.name}", jobmanagerLabels
            )

        jobmanagerPodMetadata.annotations = flinkCluster.spec.jobManager?.annotations

        return V1StatefulSetBuilder()
            .withMetadata(jobmanagerMetadata)
            .editOrNewSpec()
            .withReplicas(1)
            .editOrNewTemplate()
            .withSpec(jobmanagerPodSpec)
            .withMetadata(jobmanagerPodMetadata)
            .endTemplate()
            .withUpdateStrategy(updateStrategy)
            .withServiceName("jobmanager")
            .withSelector(jobmanagerSelector)
            .addAllToVolumeClaimTemplates(flinkCluster.spec.jobManager?.persistentVolumeClaimsTemplates ?: listOf())
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

        if (flinkCluster.spec.runtime?.image == null) {
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

        val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

        val port6121 =
            createContainerPort(
                6121,
                "data"
            )
        val port6122 =
            createContainerPort(
                6122,
                "ipc"
            )

        val podNameEnvVar =
            createEnvVarFromField(
                "POD_NAME", "metadata.name"
            )

        val podNamespaceEnvVar =
            createEnvVarFromField(
                "POD_NAMESPACE", "metadata.namespace"
            )

        val rpcAddressEnvVar =
            createEnvVar(
                "JOB_MANAGER_RPC_ADDRESS", "flink-jobmanager-${flinkCluster.metadata.name}"
            )

        val numberOfTaskSlotsEnvVar =
            createEnvVar(
                "TASK_MANAGER_NUMBER_OF_TASK_SLOTS", flinkCluster.spec.taskManager?.taskSlots?.toString() ?: "1"
            )

        val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

        val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

        val taskmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            rpcAddressEnvVar,
            numberOfTaskSlotsEnvVar
        )

        if (flinkCluster.spec.taskManager?.environment != null) {
            taskmanagerVariables.addAll(flinkCluster.spec.taskManager.environment)
        }

        val taskmanagerContainer = V1ContainerBuilder()
            .withImage(flinkCluster.spec.runtime?.image)
            .withImagePullPolicy(flinkCluster.spec.runtime?.pullPolicy ?: "Always")
            .withName("flink-taskmanager")
            .withArgs(listOf("taskmanager"))
            .addToPorts(port6121)
            .addToPorts(port6122)
            .addAllToPorts(flinkCluster.spec.taskManager?.extraPorts ?: listOf())
            .addAllToVolumeMounts(flinkCluster.spec.taskManager?.volumeMounts ?: listOf())
            .withEnv(taskmanagerVariables)
            .withEnvFrom(flinkCluster.spec.taskManager?.environmentFrom)
            .withResources(flinkCluster.spec.taskManager?.resources)
            .build()

        val taskmanagerAffinity =
            createAffinity(
                jobmanagerSelector, taskmanagerSelector
            )

        val taskmanagerPullSecrets = if (flinkCluster.spec.runtime?.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(flinkCluster.spec.runtime?.pullSecrets)
            )
        } else null

        val initContainers = flinkCluster.spec.taskManager?.initContainers ?: listOf()

        val sideContainers = flinkCluster.spec.taskManager?.sideContainers ?: listOf()

        val taskmanagerPodSpec = V1PodSpecBuilder()
            .addAllToInitContainers(initContainers)
            .addToContainers(taskmanagerContainer)
            .addAllToContainers(sideContainers)
            .withServiceAccountName(flinkCluster.spec.taskManager?.serviceAccount ?: "default")
            .withImagePullSecrets(taskmanagerPullSecrets)
            .withAffinity(taskmanagerAffinity)
            .withVolumes(flinkCluster.spec.taskManager?.volumes)
            .build()

        val taskmanagerMetadata =
            createObjectMeta(
                "flink-taskmanager-${flinkCluster.metadata.name}", taskmanagerLabels
            )

        val taskmanagerPodMetadata =
            createObjectMeta(
                "flink-taskmanager-${flinkCluster.metadata.name}", taskmanagerLabels
            )

        taskmanagerPodMetadata.annotations = flinkCluster.spec.taskManager?.annotations

        val replicas = flinkCluster.spec?.taskManagers ?: 1

        return V1StatefulSetBuilder()
            .withMetadata(taskmanagerMetadata)
            .editOrNewSpec()
            .withReplicas(replicas)
            .editOrNewTemplate()
            .withSpec(taskmanagerPodSpec)
            .withMetadata(taskmanagerPodMetadata)
            .endTemplate()
            .withUpdateStrategy(updateStrategy)
            .withServiceName("taskmanager")
            .withSelector(taskmanagerSelector)
            .addAllToVolumeClaimTemplates(flinkCluster.spec.taskManager?.persistentVolumeClaimsTemplates ?: listOf())
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

    private fun createObjectMeta(name: String, labels: Map<String, String>) = V1ObjectMeta().name(name).labels(labels)

    private fun createEnvVarFromField(name: String, fieldPath: String) =
        V1EnvVar().name(name).valueFrom(
            V1EnvVarSource().fieldRef(V1ObjectFieldSelector().fieldPath(fieldPath))
        )

    private fun createEnvVar(name: String, value: String) = V1EnvVar().name(name).value(value)

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