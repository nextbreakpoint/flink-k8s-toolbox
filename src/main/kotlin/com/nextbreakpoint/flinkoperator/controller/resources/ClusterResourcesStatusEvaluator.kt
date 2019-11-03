package com.nextbreakpoint.flinkoperator.controller.resources

import com.nextbreakpoint.flinkoperator.common.crd.V1FlinkCluster
import com.nextbreakpoint.flinkoperator.common.model.ClusterId
import com.nextbreakpoint.flinkoperator.common.model.ResourceStatus
import io.kubernetes.client.models.V1EnvVar
import kotlin.math.roundToInt

class ClusterResourcesStatusEvaluator {
    fun evaluate(
        clusterId: ClusterId,
        flinkCluster: V1FlinkCluster,
        clusterResources: ClusterResources
    ): ClusterResourcesStatus {
        val uploadJobStatus = evaluateUploadJobStatus(clusterResources, clusterId, flinkCluster)

        val jobmanagerServiceStatus = evaluateJobManagerServiceStatus(clusterResources, clusterId, flinkCluster)

        val jobmanagerStatefulSetStatus = evaluateJobManagerStatefulSetStatus(clusterResources, clusterId, flinkCluster)

        val taskmanagerStatefulSetStatus = evaluateTaskManagerStatefulSetStatus(clusterResources, clusterId, flinkCluster)

        return ClusterResourcesStatus(
            jarUploadJob = uploadJobStatus,
            jobmanagerService = jobmanagerServiceStatus,
            jobmanagerStatefulSet = jobmanagerStatefulSetStatus,
            taskmanagerStatefulSet = taskmanagerStatefulSetStatus
        )
    }

    private fun extractArgument(containerArguments: List<String>, name: String) =
        containerArguments.filter { it.startsWith(name) }.map { it.substringAfter("=") }.firstOrNull()

    private fun evaluateUploadJobStatus(
        clusterResources: ClusterResources,
        clusterId: ClusterId,
        flinkCluster: V1FlinkCluster
    ): Pair<ResourceStatus, List<String>> {
        val jarUploadJob = clusterResources.jarUploadJob ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (jarUploadJob.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jarUploadJob.metadata.labels["name"]?.equals(flinkCluster.metadata.name) != true) {
            statusReport.add("name label missing or invalid")
        }

        if (jarUploadJob.metadata.labels["uid"]?.equals(clusterId.uuid) != true) {
            statusReport.add("uid label missing or invalid")
        }

        if (jarUploadJob.spec.template.spec.serviceAccountName != "flink-upload") {
            statusReport.add("service account does not match")
        }

        if (flinkCluster.spec.flinkImage?.pullSecrets != null) {
            if (jarUploadJob.spec.template.spec.imagePullSecrets.size != 1) {
                statusReport.add("unexpected number of pull secrets")
            } else {
                if (jarUploadJob.spec.template.spec.imagePullSecrets[0].name != flinkCluster.spec.flinkImage?.pullSecrets) {
                    statusReport.add("pull secrets don't match")
                }
            }
        }

        if (jarUploadJob.spec.template.spec.containers.size != 1) {
            statusReport.add("unexpected number of containers")
        }

        if (jarUploadJob.spec.template.spec.containers.size > 0) {
            val container = jarUploadJob.spec.template.spec.containers.get(0)

            if (container.image != flinkCluster.spec.flinkJob.image) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != flinkCluster.spec.flinkImage?.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.args.size < 1 || container.args[0] != "upload") {
                statusReport.add("missing upload command: ${container.args.joinToString(separator = ",")}")
            } else {
                if (container.args.size < 2 || container.args[1] != "jar") {
                    statusReport.add("unexpected sub command: ${container.args.joinToString(separator = ",")}")
                } else {
                    val jobNamespace = extractArgument(container.args, "--namespace")

                    val jobClusterName = extractArgument(container.args, "--cluster-name")

                    val jobJarPath = extractArgument(container.args, "--jar-path")

                    if (jobNamespace == null || jobNamespace != clusterId.namespace) {
                        statusReport.add("unexpected argument namespace: ${container.args.joinToString(separator = " ")}")
                    }

                    if (jobClusterName == null || jobClusterName != flinkCluster.metadata.name) {
                        statusReport.add("unexpected argument cluster name: ${container.args.joinToString(separator = " ")}")
                    }

                    if (jobJarPath == null || jobJarPath != flinkCluster.spec.flinkJob.jarPath) {
                        statusReport.add("unexpected argument jar path: ${container.args.joinToString(separator = " ")}")
                    }
                }
            }
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }

    private fun evaluateJobManagerServiceStatus(
        actualClusterResources: ClusterResources,
        clusterId: ClusterId,
        flinkCluster: V1FlinkCluster
    ): Pair<ResourceStatus, List<String>> {
        val jobmanagerService = actualClusterResources.jobmanagerService ?: return ResourceStatus.MISSING to listOf()

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

        if (jobmanagerService.metadata.labels["uid"]?.equals(clusterId.uuid) != true) {
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

    private fun evaluateJobManagerStatefulSetStatus(
        actualClusterResources: ClusterResources,
        clusterId: ClusterId,
        flinkCluster: V1FlinkCluster
    ): Pair<ResourceStatus, List<String>> {
        val jobmanagerStatefulSet = actualClusterResources.jobmanagerStatefulSet ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (jobmanagerStatefulSet.metadata.labels["role"]?.equals("jobmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (jobmanagerStatefulSet.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jobmanagerStatefulSet.metadata.labels["name"]?.equals(flinkCluster.metadata.name) != true) {
            statusReport.add("name label missing or invalid")
        }

        if (jobmanagerStatefulSet.metadata.labels["uid"]?.equals(clusterId.uuid) != true) {
            statusReport.add("uid label missing or invalid")
        }

        if (jobmanagerStatefulSet.spec.template.spec.serviceAccountName != flinkCluster.spec.jobManager?.serviceAccount ?: "default") {
            statusReport.add("service account does not match")
        }

        if (flinkCluster.spec.flinkImage?.pullSecrets != null) {
            if (jobmanagerStatefulSet.spec.template.spec.imagePullSecrets?.size != 1) {
                statusReport.add("unexpected number of pull secrets")
            } else {
                if (jobmanagerStatefulSet.spec.template.spec.imagePullSecrets[0].name != flinkCluster.spec.flinkImage?.pullSecrets) {
                    statusReport.add("pull secrets don't match")
                }
            }
        }

        if (jobmanagerStatefulSet.spec.volumeClaimTemplates?.size != flinkCluster.spec.jobManager?.persistentVolumeClaimsTemplates?.size) {
            statusReport.add("unexpected number of volume claim templates")
        }

        val initContainerCount = flinkCluster.spec?.jobManager?.initContainers?.size ?: 0
        val sideContainerCount = flinkCluster.spec?.jobManager?.sideContainers?.size ?: 0

        if (jobmanagerStatefulSet.spec.template.spec.initContainers.size != initContainerCount) {
            statusReport.add("unexpected number of init containers")
        }

        if (jobmanagerStatefulSet.spec.template.spec.containers.size != sideContainerCount + 1) {
            statusReport.add("unexpected number of containers")
        }

        if (jobmanagerStatefulSet.spec.template.spec.containers.size > 0) {
            val container = jobmanagerStatefulSet.spec.template.spec.containers.get(0)

            if (container.image != flinkCluster.spec.flinkImage?.flinkImage) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != flinkCluster.spec.flinkImage?.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.resources.limits.get("cpu")?.number?.toFloat()?.equals(flinkCluster.spec.jobManager.requiredCPUs ?: 1.0f) != true) {
                statusReport.add("container cpu limit doesn't match")
            }

            if (container.resources.requests.get("memory")?.number?.toInt()?.equals((flinkCluster.spec.jobManager.requiredMemory ?: 256) * 1024 * 1024) != true) {
                statusReport.add("container memory limit doesn't match")
            }

            val jobmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull()

            if (jobmanagerRpcAddressEnvVar?.value == null || (actualClusterResources.jobmanagerService != null && jobmanagerRpcAddressEnvVar.value.toString() != actualClusterResources.jobmanagerService.metadata.name)) {
                statusReport.add("missing or invalid environment variable JOB_MANAGER_RPC_ADDRESS")
            }

            val jobmanagerMemoryEnvVar = container.env.filter { it.name == "FLINK_JM_HEAP" }.firstOrNull()

            if (jobmanagerMemoryEnvVar?.value == null || jobmanagerMemoryEnvVar.value.toInt() < flinkCluster.spec.jobManager.requiredMemory ?: 256) {
                statusReport.add("missing or invalid environment variable FLINK_JM_HEAP")
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
                .filter { it.name != "FLINK_JM_HEAP" }
                .filter { it.name != "POD_NAMESPACE" }
                .filter { it.name != "POD_NAME" }
                .map { it }
                .toList()

            if (jobmanagerEnvironmentVariables != flinkCluster.spec.jobManager.environment ?: listOf<V1EnvVar>()) {
                statusReport.add("container environment variables don't match")
            }
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }

    private fun evaluateTaskManagerStatefulSetStatus(
        actualClusterResources: ClusterResources,
        clusterId: ClusterId,
        flinkCluster: V1FlinkCluster
    ): Pair<ResourceStatus, List<String>> {
        val taskmanagerStatefulSet = actualClusterResources.taskmanagerStatefulSet ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (taskmanagerStatefulSet.metadata.labels["role"]?.equals("taskmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (taskmanagerStatefulSet.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (taskmanagerStatefulSet.metadata.labels["name"]?.equals(flinkCluster.metadata.name) != true) {
            statusReport.add("name label missing or invalid")
        }

        if (taskmanagerStatefulSet.metadata.labels["uid"]?.equals(clusterId.uuid) != true) {
            statusReport.add("uid label missing or invalid")
        }

        if (taskmanagerStatefulSet.spec.template.spec.serviceAccountName != flinkCluster.spec.taskManager?.serviceAccount ?: "default") {
            statusReport.add("service account does not match")
        }

        if (flinkCluster.spec.flinkImage?.pullSecrets != null) {
            if (taskmanagerStatefulSet.spec.template.spec.imagePullSecrets.size != 1) {
                statusReport.add("unexpected number of pull secrets")
            } else {
                if (taskmanagerStatefulSet.spec.template.spec.imagePullSecrets[0].name != flinkCluster.spec.flinkImage?.pullSecrets) {
                    statusReport.add("pull secrets don't match")
                }
            }
        }

        if (taskmanagerStatefulSet.spec.volumeClaimTemplates?.size != flinkCluster.spec.taskManager?.persistentVolumeClaimsTemplates?.size) {
            statusReport.add("unexpected number of volume claim templates")
        }

        val parallelism = flinkCluster.spec.flinkJob?.parallelism ?: 1
        val taskSlots = flinkCluster.spec.taskManager?.taskSlots ?: 1
        val replicas = ((parallelism + 0.5) / taskSlots).roundToInt()

        if (taskmanagerStatefulSet.spec.replicas != replicas) {
            statusReport.add("number of replicas doesn't match")
        }

        val initContainerCount = flinkCluster.spec?.jobManager?.initContainers?.size ?: 0
        val sideContainerCount = flinkCluster.spec?.jobManager?.sideContainers?.size ?: 0

        if (taskmanagerStatefulSet.spec.template.spec.initContainers.size != initContainerCount) {
            statusReport.add("unexpected number of init containers")
        }

        if (taskmanagerStatefulSet.spec.template.spec.containers.size != sideContainerCount + 1) {
            statusReport.add("unexpected number of containers")
        }

        if (taskmanagerStatefulSet.spec.template.spec.containers.size > 0) {
            val container = taskmanagerStatefulSet.spec.template.spec.containers.get(0)

            if (container.image != flinkCluster.spec.flinkImage?.flinkImage) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != flinkCluster.spec.flinkImage?.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.resources.limits.get("cpu")?.number?.toFloat()?.equals(flinkCluster.spec.taskManager.requiredCPUs ?: 1.0f) != true) {
                statusReport.add("container cpu limit doesn't match")
            }

            if (container.resources.requests.get("memory")?.number?.toInt()?.equals((flinkCluster.spec.taskManager.requiredMemory ?: 1024) * 1024 * 1024) != true) {
                statusReport.add("container memory limit doesn't match")
            }

            val taskmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull()

            if (taskmanagerRpcAddressEnvVar?.value == null || (actualClusterResources.jobmanagerService != null && taskmanagerRpcAddressEnvVar.value.toString() != actualClusterResources.jobmanagerService.metadata.name)) {
                statusReport.add("missing or invalid environment variable JOB_MANAGER_RPC_ADDRESS")
            }

            val taskmanagerMemoryEnvVar = container.env.filter { it.name == "FLINK_TM_HEAP" }.firstOrNull()

            if (taskmanagerMemoryEnvVar?.value == null || taskmanagerMemoryEnvVar.value.toInt() < flinkCluster.spec.taskManager.requiredMemory ?: 1024) {
                statusReport.add("missing or invalid environment variable FLINK_TM_HEAP")
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
                .filter { it.name != "FLINK_TM_HEAP" }
                .filter { it.name != "TASK_MANAGER_NUMBER_OF_TASK_SLOTS" }
                .filter { it.name != "POD_NAMESPACE" }
                .filter { it.name != "POD_NAME" }
                .map { it }
                .toList()

            if (!taskmanagerEnvironmentVariables.equals(flinkCluster.spec.taskManager.environment ?: listOf<V1EnvVar>())) {
                statusReport.add("container environment variables don't match")
            }
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }
}