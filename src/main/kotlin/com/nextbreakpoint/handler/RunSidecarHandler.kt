package com.nextbreakpoint.handler

import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.model.JobSubmitConfig
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.*
import org.apache.log4j.Logger
import java.util.regex.Pattern

object RunSidecarHandler {
    private val logger = Logger.getLogger(RunSidecarHandler::class.simpleName)

    private val ARGUMENTS_PATTERN = "(--([^ ]+)=(\"[^=]+\"))|(--([^ ]+)=([^\"= ]+))|([^ ]+)"

    fun execute(portForward: Int?, useNodePort: Boolean, submitConfig: JobSubmitConfig): String {
        try {
            val api = AppsV1Api()

            val results = Pattern.compile(ARGUMENTS_PATTERN).matcher(submitConfig.sidecar.arguments).results()

            deleteDeployment(api, submitConfig.descriptor)

            val environmentEnvVar = createEnvVar(
                "FLINK_ENVIRONMENT",
                submitConfig.descriptor.environment
            )

            val podNameEnvVar =
                createEnvVarFromField("POD_NAME", "metadata.name")

            val podNamespaceEnvVar = createEnvVarFromField(
                "POD_NAMESPACE",
                "metadata.namespace"
            )

            val arguments = mutableListOf<String>()

            arguments.add("sidecar")

            results.forEach { result ->
                if (result.group(7) != null) {
                    arguments.add(result.group(7))
                } else if (result.group(2) != null) {
                    arguments.add("--${result.group(2)}=${result.group(3)}")
                } else {
                    arguments.add("--${result.group(5)}=${result.group(6)}")
                }
            }

            val componentLabel = Pair("component", "flink")

            val environmentLabel = Pair("environment", submitConfig.descriptor.environment)

            val clusterLabel = Pair("cluster", submitConfig.descriptor.name)

            val jobmanagerLabel = Pair("role", "jobmanager")

            val taskmanagerLabel = Pair("role", "taskmanager")

            val jobmanagerLabels = mapOf(clusterLabel, componentLabel, jobmanagerLabel, environmentLabel)

            val taskmanagerLabels = mapOf(clusterLabel, componentLabel, taskmanagerLabel, environmentLabel)

            val sidecarLabels = mapOf(clusterLabel, componentLabel, environmentLabel)

            val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

            val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

            val sidecarSelector = V1LabelSelector().matchLabels(sidecarLabels)

            val jobmanagerAffinity =
                createAffinity(jobmanagerSelector, taskmanagerSelector)

            val sidecar = V1Container()
                .image(submitConfig.sidecar.image)
                .imagePullPolicy(submitConfig.sidecar.pullPolicy)
                .name("flink-submit-sidecar")
                .args(arguments)
                .env(
                    listOf(
                        podNameEnvVar,
                        podNamespaceEnvVar,
                        environmentEnvVar
                    )
                )
                .resources(createSidecarResourceRequirements())

            val sidecarPodSpec = V1PodSpec()
                .containers(
                    listOf(sidecar)
                )
                .serviceAccountName("flink-submit")
                .imagePullSecrets(
                    listOf(
                        V1LocalObjectReference().name(submitConfig.sidecar.pullSecrets)
                    )
                )
                .affinity(jobmanagerAffinity)

            val sidecarMetadata =
                createObjectMeta("flink-submit-sidecar-", sidecarLabels)

            val sidecarDeployment = V1Deployment()
                .metadata(sidecarMetadata)
                .spec(
                    V1DeploymentSpec()
                        .replicas(1)
                        .template(
                            V1PodTemplateSpec()
                                .spec(sidecarPodSpec)
                                .metadata(sidecarMetadata)
                        )
                        .selector(sidecarSelector)
                )

            logger.info("Creating Sidecar Deployment ...")

            val sidecarDeploymentOut = api.createNamespacedDeployment(
                submitConfig.descriptor.namespace,
                sidecarDeployment,
                null,
                null,
                null
            )

            logger.info("Deployment created ${sidecarDeploymentOut.metadata.name}")

            return "{\"status\":\"SUCCESS\"}"
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    private fun createEnvVarFromField(s: String, s1: String) = V1EnvVar()
        .name(s)
        .valueFrom(
            V1EnvVarSource()
                .fieldRef(
                    V1ObjectFieldSelector().fieldPath(s1)
                )
        )

    private fun createEnvVar(name: String, value: String) = V1EnvVar()
        .name(name)
        .value(value)

    private fun createObjectMeta(name: String, jobmanagerLabels: Map<String, String>) = V1ObjectMeta()
        .generateName(name)
        .labels(jobmanagerLabels)

    private fun createAffinity(jobmanagerSelector: V1LabelSelector?, taskmanagerSelector: V1LabelSelector?) = V1Affinity()
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

    private fun createSidecarResourceRequirements() = V1ResourceRequirements()
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

    private fun deleteDeployment(api: AppsV1Api, descriptor: ClusterDescriptor) {
        val deployments = api.listNamespacedDeployment(
            descriptor.namespace,
            null,
            null,
            null,
            null,
            "cluster=${descriptor.name},environment=${descriptor.environment}",
            null,
            null,
            30,
            null
        )

        deployments.items.forEach { deployment ->
            try {
                logger.info("Removing Deployment ${deployment.metadata.name}...")

                val status = api.deleteNamespacedDeployment(
                    deployment.metadata.name,
                    descriptor.namespace,
                    V1DeleteOptions(),
                    "true",
                    null,
                    null,
                    null,
                    null
                )

                logger.info("Response status: ${status.reason}")

                status.details.causes.forEach { logger.info(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }
}