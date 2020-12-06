package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.crd.V1SupervisorSpec
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.openapi.models.V1Affinity
import io.kubernetes.client.openapi.models.V1Container
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1DeploymentBuilder
import io.kubernetes.client.openapi.models.V1DeploymentStrategy
import io.kubernetes.client.openapi.models.V1EnvVar
import io.kubernetes.client.openapi.models.V1EnvVarSource
import io.kubernetes.client.openapi.models.V1LabelSelector
import io.kubernetes.client.openapi.models.V1LocalObjectReference
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector
import io.kubernetes.client.openapi.models.V1PodAffinity
import io.kubernetes.client.openapi.models.V1PodAffinityTerm
import io.kubernetes.client.openapi.models.V1PodTemplateSpecBuilder
import io.kubernetes.client.openapi.models.V1ResourceRequirements
import io.kubernetes.client.openapi.models.V1WeightedPodAffinityTerm

object SupervisorResourcesDefaultFactory : SupervisorResourcesFactory {
    override fun createSupervisorDeployment(
        namespace: String,
        owner: String,
        clusterName: String,
        supervisorSpec: V1SupervisorSpec,
        replicas: Int,
        dryRun: Boolean
    ): V1Deployment {
        if (supervisorSpec.image == null) {
            throw RuntimeException("image is required")
        }

        val supervisorLabels = mapOf(
            Pair("owner", owner),
            Pair("clusterName", clusterName),
            Pair("component", "flink"),
            Pair("role", "supervisor")
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
            createSupervisorArguments(
                namespace,
                clusterName,
                supervisorSpec,
                dryRun
            )

        val supervisorSelector = V1LabelSelector().matchLabels(supervisorLabels)

        val supervisorAffinity =
            createAffinity(
                supervisorSelector
            )

        val pullSecrets =
            createObjectReferenceListOrNull(
                supervisorSpec.pullSecrets
            )

        val supervisorPodSpec = V1PodTemplateSpecBuilder()
            .editOrNewMetadata()
            .withName("supervisor-$clusterName")
            .withLabels(supervisorLabels)
            .endMetadata()
            .editOrNewSpec()
            .addToContainers(V1Container())
            .editFirstContainer()
            .withName("bootstrap")
            .withImage(supervisorSpec.image)
            .withImagePullPolicy(supervisorSpec.pullPolicy ?: "IfNotPresent")
            .withArgs(arguments)
            .addToEnv(podNameEnvVar)
            .addToEnv(podNamespaceEnvVar)
            .withResources(supervisorSpec.resources ?: createResourceRequirements())
            .endContainer()
            .withServiceAccountName(supervisorSpec.serviceAccount ?: "default")
            .withImagePullSecrets(pullSecrets)
            .withRestartPolicy("Always")
            .withAffinity(supervisorAffinity)
            .endSpec()
            .build()

        val deployment = V1DeploymentBuilder()
            .editOrNewMetadata()
            .withName("supervisor-$clusterName")
            .withLabels(supervisorLabels)
            .addToAnnotations("flink-operator/deployment-digest", Resource.computeDigest(supervisorSpec))
            .endMetadata()
            .editOrNewSpec()
            .withTemplate(supervisorPodSpec)
            .withSelector(supervisorSelector)
            .withStrategy(V1DeploymentStrategy().type("Recreate"))
            .withReplicas(if (dryRun) 0 else replicas)
            .withProgressDeadlineSeconds(180)
            .withMinReadySeconds(30)
            .withRevisionHistoryLimit(3)
            .endSpec()
            .build()

        return deployment
    }

    private fun createAffinity(
        selector: V1LabelSelector?
    ): V1Affinity = V1Affinity()
        .podAffinity(
            V1PodAffinity().preferredDuringSchedulingIgnoredDuringExecution(
                listOf(
                    V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                        V1PodAffinityTerm()
                            .topologyKey("kubernetes.io/hostname")
                            .labelSelector(selector)
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
                "cpu" to Quantity("1.0"),
                "memory" to Quantity("256Mi")
            )
        )
        .requests(
            mapOf(
                "cpu" to Quantity("0.1"),
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

    private fun createSupervisorArguments(
        namespace: String,
        clusterName: String,
        supervisor: V1SupervisorSpec,
        dryRun: Boolean
    ): List<String> {
        val arguments = mutableListOf<String>()

        arguments.addAll(
            listOf(
                "supervisor",
                "run",
                "--namespace=$namespace",
                "--cluster-name=$clusterName",
                "--polling-interval=${supervisor.pollingInterval ?: 5}",
                "--task-timeout=${supervisor.taskTimeout ?: 300}"
            )
        )

        if (dryRun) {
            arguments.add("--dry-run")
        }

        return arguments.toList()
    }
}