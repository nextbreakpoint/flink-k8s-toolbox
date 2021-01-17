package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import io.kubernetes.client.custom.IntOrString
import io.kubernetes.client.openapi.models.V1Affinity
import io.kubernetes.client.openapi.models.V1ContainerBuilder
import io.kubernetes.client.openapi.models.V1ContainerPort
import io.kubernetes.client.openapi.models.V1EnvVar
import io.kubernetes.client.openapi.models.V1EnvVarSource
import io.kubernetes.client.openapi.models.V1HTTPGetAction
import io.kubernetes.client.openapi.models.V1LabelSelector
import io.kubernetes.client.openapi.models.V1LocalObjectReference
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector
import io.kubernetes.client.openapi.models.V1ObjectMeta
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1PodAffinityTerm
import io.kubernetes.client.openapi.models.V1PodAntiAffinity
import io.kubernetes.client.openapi.models.V1PodBuilder
import io.kubernetes.client.openapi.models.V1Probe
import io.kubernetes.client.openapi.models.V1Service
import io.kubernetes.client.openapi.models.V1ServiceBuilder
import io.kubernetes.client.openapi.models.V1ServicePort
import io.kubernetes.client.openapi.models.V1WeightedPodAffinityTerm

object ClusterResourcesDefaultFactory : ClusterResourcesFactory {
    override fun createService(
        namespace: String,
        owner: String,
        clusterName: String,
        clusterSpec: V1FlinkClusterSpec
    ): V1Service {
        val serviceLabels = mapOf(
            Pair("owner", owner),
            Pair("clusterName", clusterName),
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
            .withName("jobmanager-$clusterName")
            .withLabels(serviceLabels)
            .endMetadata()
            .editOrNewSpec()
            .addToPorts(srvPort8081)
            .addToPorts(srvPort6123)
            .addToPorts(srvPort6124)
            .addToPorts(srvPort6125)
            .withSelector(serviceLabels)
            .withType(clusterSpec.jobManager?.serviceMode ?: "ClusterIP")
            .endSpec()
            .build()
    }

    override fun createJobManagerPod(
        namespace: String,
        owner: String,
        clusterName: String,
        clusterSpec: V1FlinkClusterSpec
    ): V1Pod {
        if (clusterSpec.runtime?.image == null) {
            throw RuntimeException("flinkImage is required")
        }

        val jobmanagerLabels = mapOf(
            Pair("owner", owner),
            Pair("clusterName", clusterName),
            Pair("component", "flink"),
            Pair("role", "jobmanager")
        )

        val taskmanagerLabels = mapOf(
            Pair("owner", owner),
            Pair("clusterName", clusterName),
            Pair("component", "flink"),
            Pair("role", "taskmanager")
        )

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
                "JOB_MANAGER_RPC_ADDRESS", "jobmanager-$clusterName"
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

        clusterSpec.jobManager?.environment?.let { jobmanagerVariables.addAll(it) }

        val jobmanagerContainer = V1ContainerBuilder()
            .withImage(clusterSpec.runtime?.image)
            .withImagePullPolicy(clusterSpec.runtime?.pullPolicy ?: "IfNotPresent")
            .withName("jobmanager")
            .withCommand(clusterSpec.jobManager?.command)
            .withArgs(clusterSpec.jobManager?.args ?: listOf("jobmanager"))
            .addToPorts(port8081)
            .addToPorts(port6123)
            .addToPorts(port6124)
            .addToPorts(port6125)
            .addAllToPorts(clusterSpec.jobManager?.extraPorts ?: listOf())
            .addAllToVolumeMounts(clusterSpec.jobManager?.volumeMounts ?: listOf())
            .withEnv(jobmanagerVariables)
            .withEnvFrom(clusterSpec.jobManager?.environmentFrom)
            .withResources(clusterSpec.jobManager?.resources)
//            .withReadinessProbe(
//                V1Probe()
//                    .httpGet(V1HTTPGetAction().port(IntOrString(8081)).path("/overview"))
//                    .initialDelaySeconds(15)
//                    .periodSeconds(30)
//                    .failureThreshold(3)
//                    .successThreshold(1)
//            )
            .withLivenessProbe(
                V1Probe()
                    .httpGet(V1HTTPGetAction().port(IntOrString(8081)).path("/overview"))
                    .initialDelaySeconds(15)
                    .periodSeconds(30)
                    .failureThreshold(3)
                    .successThreshold(1)
            )
            .build()

        val jobmanagerPullSecrets = createObjectReferenceListOrNull(clusterSpec.runtime?.pullSecrets)

        val initContainers = clusterSpec.jobManager?.initContainers ?: listOf()

        val sideContainers = clusterSpec.jobManager?.sideContainers ?: listOf()

        val jobmanagerMetadata =
            createObjectMeta(
                "jobmanager-$clusterName-", jobmanagerLabels
            )

        jobmanagerMetadata.annotations = clusterSpec.jobManager?.annotations

        return V1PodBuilder()
            .withMetadata(jobmanagerMetadata)
            .editOrNewSpec()
            .addAllToInitContainers(initContainers)
            .addToContainers(jobmanagerContainer)
            .addAllToContainers(sideContainers)
            .withServiceAccountName(clusterSpec.jobManager?.serviceAccount ?: "default")
            .withImagePullSecrets(jobmanagerPullSecrets)
            .withVolumes(clusterSpec.jobManager?.volumes)
            .withAffinity(clusterSpec.jobManager?.affinity ?: jobmanagerAffinity)
            .withTolerations(clusterSpec.jobManager?.tolerations)
            .withTopologySpreadConstraints(clusterSpec.jobManager?.topologySpreadConstraints)
            .endSpec()
            .build()
    }

    override fun createTaskManagerPod(
        namespace: String,
        owner: String,
        clusterName: String,
        clusterSpec: V1FlinkClusterSpec
    ): V1Pod {
        if (clusterSpec.runtime?.image == null) {
            throw RuntimeException("flinkImage is required")
        }

        val jobmanagerLabels = mapOf(
            Pair("owner", owner),
            Pair("clusterName", clusterName),
            Pair("component", "flink"),
            Pair("role", "jobmanager")
        )

        val taskmanagerLabels = mapOf(
            Pair("owner", owner),
            Pair("clusterName", clusterName),
            Pair("component", "flink"),
            Pair("role", "taskmanager")
        )

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
                "JOB_MANAGER_RPC_ADDRESS", "jobmanager-$clusterName"
            )

        val numberOfTaskSlotsEnvVar =
            createEnvVar(
                "TASK_MANAGER_NUMBER_OF_TASK_SLOTS", clusterSpec.taskManager?.taskSlots?.toString() ?: "1"
            )

        val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

        val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

        val taskmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            rpcAddressEnvVar,
            numberOfTaskSlotsEnvVar
        )

        clusterSpec.taskManager?.environment?.let { taskmanagerVariables.addAll(it) }

        val taskmanagerContainer = V1ContainerBuilder()
            .withImage(clusterSpec.runtime?.image)
            .withImagePullPolicy(clusterSpec.runtime?.pullPolicy ?: "IfNotPresent")
            .withName("taskmanager")
            .withCommand(clusterSpec.taskManager?.command)
            .withArgs(clusterSpec.taskManager?.args ?: listOf("taskmanager"))
            .addToPorts(port6121)
            .addToPorts(port6122)
            .addAllToPorts(clusterSpec.taskManager?.extraPorts ?: listOf())
            .addAllToVolumeMounts(clusterSpec.taskManager?.volumeMounts ?: listOf())
            .withEnv(taskmanagerVariables)
            .withEnvFrom(clusterSpec.taskManager?.environmentFrom)
            .withResources(clusterSpec.taskManager?.resources)
            .build()

        val taskmanagerAffinity =
            createAffinity(
                jobmanagerSelector, taskmanagerSelector
            )

        val taskmanagerPullSecrets = createObjectReferenceListOrNull(clusterSpec.runtime?.pullSecrets)

        val initContainers = clusterSpec.taskManager?.initContainers ?: listOf()

        val sideContainers = clusterSpec.taskManager?.sideContainers ?: listOf()

        val taskmanagerMetadata =
            createObjectMeta(
                "taskmanager-$clusterName-", taskmanagerLabels
            )

        taskmanagerMetadata.annotations = clusterSpec.taskManager?.annotations

        return V1PodBuilder()
            .withMetadata(taskmanagerMetadata)
            .editOrNewSpec()
            .addAllToInitContainers(initContainers)
            .addToContainers(taskmanagerContainer)
            .addAllToContainers(sideContainers)
            .withServiceAccountName(clusterSpec.taskManager?.serviceAccount ?: "default")
            .withImagePullSecrets(taskmanagerPullSecrets)
            .withVolumes(clusterSpec.taskManager?.volumes)
            .withAffinity(clusterSpec.taskManager?.affinity ?: taskmanagerAffinity)
            .withTolerations(clusterSpec.taskManager?.tolerations)
            .withTopologySpreadConstraints(clusterSpec.taskManager?.topologySpreadConstraints)
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

    private fun createObjectReferenceListOrNull(referenceName: String?): List<V1LocalObjectReference>? {
        return if (referenceName != null) {
            listOf(
                V1LocalObjectReference().name(referenceName)
            )
        } else null
    }

    private fun createObjectMeta(name: String, labels: Map<String, String>) = V1ObjectMeta().generateName(name).labels(labels)

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