package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterSelector
import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus
import io.kubernetes.client.models.V1EnvVar

class ClusterResourcesValidator {
    fun evaluate(
        clusterSelector: ClusterSelector,
        flinkCluster: V1FlinkCluster,
        clusterResources: ClusterResources
    ): ClusterResourcesStatus {
        val jobmanagerServiceStatus = evaluateJobManagerServiceStatus(clusterResources, clusterSelector, flinkCluster)

        val jobmanagerStatefulSetStatus = evaluateJobManagerPodStatus(clusterResources, clusterSelector, flinkCluster)

        val taskmanagerStatefulSetStatus = evaluateTaskManagerPodStatus(clusterResources, clusterSelector, flinkCluster)

        return ClusterResourcesStatus(
            service = jobmanagerServiceStatus,
            jobmanagerPod = jobmanagerStatefulSetStatus,
            taskmanagerPod = taskmanagerStatefulSetStatus
        )
    }

    private fun evaluateJobManagerServiceStatus(
        actualClusterResources: ClusterResources,
        clusterSelector: ClusterSelector,
        flinkCluster: V1FlinkCluster
    ): Pair<ResourceStatus, List<String>> {
        val jobmanagerService = actualClusterResources.service ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (jobmanagerService.metadata.labels["role"]?.equals("jobmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (jobmanagerService.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jobmanagerService.metadata.labels["name"]?.equals(flinkCluster.metadata.name) != true) {
            statusReport.add("name label missing or invalid")
        }

        if (jobmanagerService.metadata.labels["uid"]?.equals(clusterSelector.uuid) != true) {
            statusReport.add("uid label missing or invalid")
        }

        if (jobmanagerService.spec.type != flinkCluster.spec.jobManager.serviceMode ?: "ClusterIP") {
            statusReport.add("service mode doesn't match")
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }

    private fun evaluateJobManagerPodStatus(
        actualClusterResources: ClusterResources,
        clusterSelector: ClusterSelector,
        flinkCluster: V1FlinkCluster
    ): Pair<ResourceStatus, List<String>> {
        val jobmanagerPod = actualClusterResources.jobmanagerPod ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (jobmanagerPod.metadata.labels["role"]?.equals("jobmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (jobmanagerPod.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jobmanagerPod.metadata.labels["name"]?.equals(flinkCluster.metadata.name) != true) {
            statusReport.add("name label missing or invalid")
        }

        if (jobmanagerPod.metadata.labels["uid"]?.equals(clusterSelector.uuid) != true) {
            statusReport.add("uid label missing or invalid")
        }

        if (jobmanagerPod.spec.serviceAccountName != flinkCluster.spec.jobManager?.serviceAccount ?: "default") {
            statusReport.add("service account does not match")
        }

        if (flinkCluster.spec.runtime?.pullSecrets != null) {
            if (jobmanagerPod.spec.imagePullSecrets?.size != 1) {
                statusReport.add("unexpected number of pull secrets")
            } else {
                if (jobmanagerPod.spec.imagePullSecrets[0].name != flinkCluster.spec.runtime?.pullSecrets) {
                    statusReport.add("pull secrets don't match")
                }
            }
        }

        val initContainerCount = flinkCluster.spec?.jobManager?.initContainers?.size ?: 0
        val sideContainerCount = flinkCluster.spec?.jobManager?.sideContainers?.size ?: 0

        if (jobmanagerPod.spec.initContainers?.size != initContainerCount) {
            statusReport.add("unexpected number of init containers")
        }

        if (jobmanagerPod.spec.containers?.size == sideContainerCount + 1) {
            val container = jobmanagerPod.spec.containers.get(0)

            if (container.image != flinkCluster.spec.runtime?.image) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != flinkCluster.spec.runtime?.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.resources != flinkCluster.spec.jobManager?.resources) {
                statusReport.add("container resources don't match")
            }

            val jobmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull()

            if (jobmanagerRpcAddressEnvVar?.value == null || (actualClusterResources.service != null && jobmanagerRpcAddressEnvVar.value.toString() != actualClusterResources.service.metadata.name)) {
                statusReport.add("missing or invalid environment variable JOB_MANAGER_RPC_ADDRESS")
            }

            val jobmanagerPodNamespaceEnvVar = container.env.filter { it.name == "POD_NAMESPACE" }.firstOrNull()

            if (jobmanagerPodNamespaceEnvVar?.valueFrom == null || jobmanagerPodNamespaceEnvVar.valueFrom.fieldRef.fieldPath != "metadata.namespace") {
                statusReport.add("missing or invalid environment variable POD_NAMESPACE")
            }

            val jobmanagerPodNameEnvVar = container.env.filter { it.name == "POD_NAME" }.firstOrNull()

            if (jobmanagerPodNameEnvVar?.valueFrom == null || jobmanagerPodNameEnvVar.valueFrom.fieldRef.fieldPath != "metadata.name") {
                statusReport.add("missing or invalid environment variable POD_NAME")
            }

            val jobmanagerEnvironmentVariables = container.env
                .filter { it.name != "JOB_MANAGER_RPC_ADDRESS" }
                .filter { it.name != "POD_NAMESPACE" }
                .filter { it.name != "POD_NAME" }
                .map { it }
                .toList()

            if (jobmanagerEnvironmentVariables != flinkCluster.spec.jobManager.environment ?: listOf<V1EnvVar>()) {
                statusReport.add("container environment variables don't match")
            }
        } else {
            statusReport.add("unexpected number of containers")
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }

    private fun evaluateTaskManagerPodStatus(
        actualClusterResources: ClusterResources,
        clusterSelector: ClusterSelector,
        flinkCluster: V1FlinkCluster
    ): Pair<ResourceStatus, List<String>> {
        val taskmanagerPod = actualClusterResources.taskmanagerPod ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (taskmanagerPod.metadata.labels["role"]?.equals("taskmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (taskmanagerPod.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (taskmanagerPod.metadata.labels["name"]?.equals(flinkCluster.metadata.name) != true) {
            statusReport.add("name label missing or invalid")
        }

        if (taskmanagerPod.metadata.labels["uid"]?.equals(clusterSelector.uuid) != true) {
            statusReport.add("uid label missing or invalid")
        }

        if (taskmanagerPod.spec.serviceAccountName != flinkCluster.spec.taskManager?.serviceAccount ?: "default") {
            statusReport.add("service account does not match")
        }

        if (flinkCluster.spec.runtime?.pullSecrets != null) {
            if (taskmanagerPod.spec.imagePullSecrets.size != 1) {
                statusReport.add("unexpected number of pull secrets")
            } else {
                if (taskmanagerPod.spec.imagePullSecrets[0].name != flinkCluster.spec.runtime?.pullSecrets) {
                    statusReport.add("pull secrets don't match")
                }
            }
        }

        val initContainerCount = flinkCluster.spec?.jobManager?.initContainers?.size ?: 0
        val sideContainerCount = flinkCluster.spec?.jobManager?.sideContainers?.size ?: 0

        if (taskmanagerPod.spec.initContainers?.size != initContainerCount) {
            statusReport.add("unexpected number of init containers")
        }

        if (taskmanagerPod.spec.containers?.size == sideContainerCount + 1) {
            val container = taskmanagerPod.spec.containers.get(0)

            if (container.image != flinkCluster.spec.runtime?.image) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != flinkCluster.spec.runtime?.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.resources != flinkCluster.spec.taskManager?.resources) {
                statusReport.add("container resources don't match")
            }

            val taskmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull()

            if (taskmanagerRpcAddressEnvVar?.value == null || (actualClusterResources.service != null && taskmanagerRpcAddressEnvVar.value.toString() != actualClusterResources.service.metadata.name)) {
                statusReport.add("missing or invalid environment variable JOB_MANAGER_RPC_ADDRESS")
            }

            val taskmanagerTaskSlotsEnvVar = container.env.filter { it.name == "TASK_MANAGER_NUMBER_OF_TASK_SLOTS" }.firstOrNull()

            if (taskmanagerTaskSlotsEnvVar?.value == null || taskmanagerTaskSlotsEnvVar.value.toInt() != flinkCluster.spec.taskManager.taskSlots ?: 1) {
                statusReport.add("missing or invalid environment variable TASK_MANAGER_NUMBER_OF_TASK_SLOTS")
            }

            val taskmanagerPodNamespaceEnvVar = container.env.filter { it.name == "POD_NAMESPACE" }.firstOrNull()

            if (taskmanagerPodNamespaceEnvVar?.valueFrom == null || taskmanagerPodNamespaceEnvVar.valueFrom.fieldRef.fieldPath != "metadata.namespace") {
                statusReport.add("missing or invalid environment variable POD_NAMESPACE")
            }

            val taskmanagerPodNameEnvVar = container.env.filter { it.name == "POD_NAME" }.firstOrNull()

            if (taskmanagerPodNameEnvVar?.valueFrom == null || taskmanagerPodNameEnvVar.valueFrom.fieldRef.fieldPath != "metadata.name") {
                statusReport.add("missing or invalid environment variable POD_NAME")
            }

            val taskmanagerEnvironmentVariables = container.env
                .filter { it.name != "JOB_MANAGER_RPC_ADDRESS" }
                .filter { it.name != "TASK_MANAGER_NUMBER_OF_TASK_SLOTS" }
                .filter { it.name != "POD_NAMESPACE" }
                .filter { it.name != "POD_NAME" }
                .map { it }
                .toList()

            if (!taskmanagerEnvironmentVariables.equals(flinkCluster.spec.taskManager.environment ?: listOf<V1EnvVar>())) {
                statusReport.add("container environment variables don't match")
            }
        } else {
            statusReport.add("unexpected number of containers")
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }
}
