package com.nextbreakpoint.flink.k8s.factory

import com.nextbreakpoint.flink.k8s.crd.V1BootstrapSpec
import com.nextbreakpoint.flink.common.ResourceSelector
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.openapi.models.V1Affinity
import io.kubernetes.client.openapi.models.V1Container
import io.kubernetes.client.openapi.models.V1EnvVar
import io.kubernetes.client.openapi.models.V1EnvVarSource
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1JobBuilder
import io.kubernetes.client.openapi.models.V1LabelSelector
import io.kubernetes.client.openapi.models.V1LocalObjectReference
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector
import io.kubernetes.client.openapi.models.V1PodAffinity
import io.kubernetes.client.openapi.models.V1PodAffinityTerm
import io.kubernetes.client.openapi.models.V1PodSpecBuilder
import io.kubernetes.client.openapi.models.V1ResourceRequirements
import io.kubernetes.client.openapi.models.V1WeightedPodAffinityTerm

object BootstrapResourcesDefaultFactory : BootstrapResourcesFactory {
    override fun createBootstrapJob(
        clusterSelector: ResourceSelector,
        jobSelector: ResourceSelector,
        clusterOwner: String,
        jobName: String,
        bootstrap: V1BootstrapSpec,
        savepointPath: String?,
        parallelism: Int,
        dryRun: Boolean
    ): V1Job {
        if (bootstrap.image == null) {
            throw RuntimeException("image is required")
        }

        if (bootstrap.jarPath == null) {
            throw RuntimeException("jarPath is required")
        }

        val jobLabels = mapOf(
            Pair("owner", clusterOwner),
            Pair("clusterName", clusterSelector.name),
            Pair("clusterUid", clusterSelector.uid),
            Pair("jobName", jobName),
            Pair("component", "flink"),
            Pair("job", "bootstrap")
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
                clusterSelector.namespace,
                clusterSelector.name,
                jobSelector.name,
                bootstrap,
                savepointPath,
                parallelism,
                dryRun
            )

        val jobAffinity =
            createAffinity(
                V1LabelSelector().matchLabels(jobLabels)
            )

        val pullSecrets =
            createObjectReferenceListOrNull(
                bootstrap.pullSecrets
            )

        val jobPodSpec = V1PodSpecBuilder()
            .addToContainers(V1Container())
            .editFirstContainer()
            .withName("bootstrap")
            .withImage(bootstrap.image)
            .withImagePullPolicy(bootstrap.pullPolicy ?: "IfNotPresent")
            .withArgs(arguments)
            .addToEnv(podNameEnvVar)
            .addToEnv(podNamespaceEnvVar)
            .withResources(bootstrap.resources ?: createResourceRequirements())
            .endContainer()
            .withServiceAccountName(bootstrap.serviceAccount ?: "default")
            .withImagePullSecrets(pullSecrets)
            .withRestartPolicy("OnFailure")
            .withAffinity(jobAffinity)
            .build()

        val job = V1JobBuilder()
            .editOrNewMetadata()
            .withGenerateName("bootstrap-${jobSelector.name}-")
            .withLabels(jobLabels)
            .endMetadata()
            .editOrNewSpec()
            .withCompletions(if (dryRun) 0 else 1)
            .withParallelism(1)
            .withBackoffLimit(1)
            .withTtlSecondsAfterFinished(30)
            .editOrNewTemplate()
            .editOrNewMetadata()
            .withGenerateName("bootstrap-${jobSelector.name}-")
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

    private fun createBootstrapArguments(
        namespace: String,
        clusterName: String,
        jobName: String,
        bootstrap: V1BootstrapSpec,
        savepointPath: String?,
        parallelism: Int,
        dryRun: Boolean
    ): List<String> {
        val arguments = mutableListOf<String>()

        arguments.addAll(
            listOf(
                "bootstrap",
                "run",
                "--namespace=$namespace",
                "--cluster-name=$clusterName",
                "--job-name=$jobName",
                "--jar-path=${bootstrap.jarPath}",
                "--class-name=${bootstrap.className}",
                "--parallelism=$parallelism"
            )
        )

        if (dryRun) {
            arguments.add("--dry-run")
        }

        if (savepointPath != null && savepointPath != "") {
            arguments.add("--savepoint-path=$savepointPath")
        }

        bootstrap.arguments.forEach {
            arguments.add("--argument=$it")
        }

        return arguments.toList()
    }
}