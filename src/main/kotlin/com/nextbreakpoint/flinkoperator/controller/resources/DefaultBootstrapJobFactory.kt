package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1BootstrapSpec
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.V1Affinity
import io.kubernetes.client.models.V1Container
import io.kubernetes.client.models.V1EnvVar
import io.kubernetes.client.models.V1EnvVarSource
import io.kubernetes.client.models.V1Job
import io.kubernetes.client.models.V1JobBuilder
import io.kubernetes.client.models.V1LabelSelector
import io.kubernetes.client.models.V1LocalObjectReference
import io.kubernetes.client.models.V1ObjectFieldSelector
import io.kubernetes.client.models.V1PodAffinity
import io.kubernetes.client.models.V1PodAffinityTerm
import io.kubernetes.client.models.V1PodSpecBuilder
import io.kubernetes.client.models.V1ResourceRequirements
import io.kubernetes.client.models.V1WeightedPodAffinityTerm

object DefaultBootstrapJobFactory : BootstrapJobFactory {
    override fun createBootstrapJob(
        clusterId: ClusterId,
        clusterOwner: String,
        bootstrap: V1BootstrapSpec
    ): V1Job {
        if (bootstrap.image == null) {
            throw RuntimeException("image is required")
        }

        if (bootstrap.jarPath == null) {
            throw RuntimeException("jarPath is required")
        }

        val jobLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("name", clusterId.name),
            Pair("uid", clusterId.uuid),
            Pair("component", "flink")
        )

        val podNameEnvVar =
            createEnvVarFromField(
                "POD_NAME", "metadata.name"
            )

        val podNamespaceEnvVar =
            createEnvVarFromField(
                "POD_NAMESPACE", "metadata.namespace"
            )

        val arguments =
            createBootstrapArguments(
                clusterId.namespace, clusterId.name, bootstrap.jarPath
            )

        val jobSelector = V1LabelSelector().matchLabels(jobLabels)

        val jobAffinity =
            createAffinity(
                jobSelector
            )

        val pullSecrets =
            createObjectReferenceListOrNull(
                bootstrap?.pullSecrets
            )

        val jobPodSpec = V1PodSpecBuilder()
            .addToContainers(V1Container())
            .editFirstContainer()
            .withName("flink-bootstrap")
            .withImage(bootstrap.image)
            .withImagePullPolicy(bootstrap?.pullPolicy ?: "Always")
            .withArgs(arguments)
            .addToEnv(podNameEnvVar)
            .addToEnv(podNamespaceEnvVar)
            .withResources(createResourceRequirements())
            .endContainer()
            .withServiceAccountName(bootstrap?.serviceAccount ?: "default")
            .withImagePullSecrets(pullSecrets)
            .withRestartPolicy("OnFailure")
            .withAffinity(jobAffinity)
            .build()

        val job = V1JobBuilder()
            .editOrNewMetadata()
            .withName("flink-bootstrap-${clusterId.name}")
            .withLabels(jobLabels)
            .endMetadata()
            .editOrNewSpec()
            .withCompletions(1)
            .withParallelism(1)
            .withBackoffLimit(3)
            .withTtlSecondsAfterFinished(30)
            .editOrNewTemplate()
            .editOrNewMetadata()
            .withName("flink-bootstrap-${clusterId.name}")
            .withLabels(jobLabels)
            .endMetadata()
            .withSpec(jobPodSpec)
            .endTemplate()
            .endSpec()
            .build()

        return job
    }

    private fun createAffinity(
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

    private fun createEnvVarFromField(name: String, fieldPath: String) =
        V1EnvVar().name(name).valueFrom(
            V1EnvVarSource().fieldRef(V1ObjectFieldSelector().fieldPath(fieldPath))
        )

    private fun createResourceRequirements() = V1ResourceRequirements()
        .limits(
            mapOf(
                "cpu" to Quantity("1"),
                "memory" to Quantity("512Mi")
            )
        )
        .requests(
            mapOf(
                "cpu" to Quantity("0.2"),
                "memory" to Quantity("256Mi")
            )
        )

    private fun createObjectReferenceListOrNull(referenceName: String?): List<V1LocalObjectReference>? {
        return if (referenceName != null) {
            listOf(
                V1LocalObjectReference().name(referenceName)
            )
        } else null
    }

    private fun createBootstrapArguments(
        namespace: String,
        clusterName: String,
        jarPath: String
    ): List<String> {
        val arguments = mutableListOf<String>()

        arguments.addAll(
            listOf(
                "bootstrap",
                "upload",
                "--namespace=$namespace",
                "--cluster-name=$clusterName",
                "--jar-path=$jarPath"
            )
        )

        return arguments.toList()
    }
}