package com.nextbreakpoint.flinkoperator.server.factory

import com.nextbreakpoint.flinkoperator.common.crd.V1OperatorSpec
import com.nextbreakpoint.flinkoperator.common.ClusterSelector
import com.nextbreakpoint.flinkoperator.server.common.Resource
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.V1Affinity
import io.kubernetes.client.models.V1Container
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1DeploymentBuilder
import io.kubernetes.client.models.V1DeploymentStrategy
import io.kubernetes.client.models.V1EnvVar
import io.kubernetes.client.models.V1EnvVarSource
import io.kubernetes.client.models.V1LabelSelector
import io.kubernetes.client.models.V1LocalObjectReference
import io.kubernetes.client.models.V1ObjectFieldSelector
import io.kubernetes.client.models.V1PodAffinity
import io.kubernetes.client.models.V1PodAffinityTerm
import io.kubernetes.client.models.V1PodTemplateSpecBuilder
import io.kubernetes.client.models.V1ResourceRequirements
import io.kubernetes.client.models.V1WeightedPodAffinityTerm

object SupervisorResourcesDefaultFactory : SupervisorResourcesFactory {
    override fun createSupervisorDeployment(
        clusterSelector: ClusterSelector,
        clusterOwner: String,
        operator: V1OperatorSpec
    ): V1Deployment {
        if (operator.image == null) {
            throw RuntimeException("image is required")
        }

        val supervisorLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("name", clusterSelector.name),
            Pair("uid", clusterSelector.uuid),
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
                clusterSelector.namespace,
                clusterSelector.name,
                operator
            )

        val supervisorSelector = V1LabelSelector().matchLabels(supervisorLabels)

        val supervisorAffinity =
            createAffinity(
                supervisorSelector
            )

        val pullSecrets =
            createObjectReferenceListOrNull(
                operator.pullSecrets
            )

        val supervisorPodSpec = V1PodTemplateSpecBuilder()
            .editOrNewMetadata()
            .withName("supervisor-${clusterSelector.name}")
            .withLabels(supervisorLabels)
            .endMetadata()
            .editOrNewSpec()
            .addToContainers(V1Container())
            .editFirstContainer()
            .withName("bootstrap")
            .withImage(operator.image)
            .withImagePullPolicy(operator.pullPolicy ?: "IfNotPresent")
            .withArgs(arguments)
            .addToEnv(podNameEnvVar)
            .addToEnv(podNamespaceEnvVar)
            .withResources(operator.resources ?: createResourceRequirements())
            .endContainer()
            .withServiceAccountName(operator.serviceAccount ?: "default")
            .withImagePullSecrets(pullSecrets)
            .withRestartPolicy("Always")
            .withAffinity(supervisorAffinity)
            .endSpec()
            .build()

        val deployment = V1DeploymentBuilder()
            .editOrNewMetadata()
            .withName("supervisor-${clusterSelector.name}")
            .withLabels(supervisorLabels)
            .addToAnnotations("flink-operator/deployment-digest", Resource.computeDigest(operator))
            .endMetadata()
            .editOrNewSpec()
            .withTemplate(supervisorPodSpec)
            .withSelector(supervisorSelector)
            .withStrategy(V1DeploymentStrategy().type("Recreate"))
            .withReplicas(1)
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
        operator: V1OperatorSpec
    ): List<String> {
        val arguments = mutableListOf<String>()

        arguments.addAll(
            listOf(
                "supervisor",
                "run",
                "--namespace=$namespace",
                "--cluster-name=$clusterName",
                "--polling-interval=${operator.pollingInterval ?: 5}",
                "--task-timeout=${operator.taskTimeout ?: 300}"
            )
        )

        return arguments.toList()
    }
}