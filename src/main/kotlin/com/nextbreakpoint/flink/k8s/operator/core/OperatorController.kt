package com.nextbreakpoint.flink.k8s.operator.core

import com.nextbreakpoint.flink.common.ClusterStatus
import com.nextbreakpoint.flink.common.ServerConfig
import com.nextbreakpoint.flink.k8s.common.FlinkClusterStatus
import com.nextbreakpoint.flink.k8s.common.FlinkDeploymentStatus
import com.nextbreakpoint.flink.k8s.common.FlinkJobStatus
import com.nextbreakpoint.flink.k8s.common.Resource
import com.nextbreakpoint.flink.k8s.controller.Controller
import com.nextbreakpoint.flink.k8s.controller.core.Result
import com.nextbreakpoint.flink.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.k8s.crd.V1FlinkClusterSpec
import com.nextbreakpoint.flink.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.k8s.crd.V1FlinkJobSpec
import com.nextbreakpoint.flink.k8s.factory.DeploymentResourcesDefaultFactory
import com.nextbreakpoint.flink.k8s.factory.SupervisorResourcesDefaultFactory
import io.kubernetes.client.openapi.models.V1Deployment

class OperatorController(
    val namespace: String,
    private val controller: Controller,
    private val serverConfig: ServerConfig
) {
    fun timeSinceLastUpdateInSeconds(deployment: V1FlinkDeployment) = (controller.currentTimeMillis() - FlinkDeploymentStatus.getStatusTimestamp(deployment).millis) / 1000L

    fun timeSinceLastUpdateInSeconds(cluster: V1FlinkCluster) = (controller.currentTimeMillis() - FlinkClusterStatus.getStatusTimestamp(cluster).millis) / 1000L

    fun timeSinceLastUpdateInSeconds(job: V1FlinkJob) = (controller.currentTimeMillis() - FlinkJobStatus.getStatusTimestamp(job).millis) / 1000L

    fun hasFinalizer(cluster: V1FlinkCluster) =
        cluster.metadata.finalizers.orEmpty().contains(Resource.OPERATOR_FINALIZER_VALUE)

    fun addFinalizer(cluster: V1FlinkCluster) {
        val finalizers = cluster.metadata.finalizers ?: listOf()
        if (!finalizers.contains(Resource.OPERATOR_FINALIZER_VALUE)) {
            cluster.metadata.finalizers = finalizers.plus(Resource.OPERATOR_FINALIZER_VALUE)
        }
    }

    fun removeFinalizer(cluster: V1FlinkCluster) {
        val finalizers = cluster.metadata.finalizers
        if (finalizers != null && finalizers.contains(Resource.OPERATOR_FINALIZER_VALUE)) {
            cluster.metadata.finalizers = finalizers.minus(Resource.OPERATOR_FINALIZER_VALUE)
        }
    }

    fun hasFinalizer(job: V1FlinkJob) =
        job.metadata.finalizers.orEmpty().contains(Resource.OPERATOR_FINALIZER_VALUE)

    fun addFinalizer(job: V1FlinkJob) {
        val finalizers = job.metadata.finalizers ?: listOf()
        if (!finalizers.contains(Resource.OPERATOR_FINALIZER_VALUE)) {
            job.metadata.finalizers = finalizers.plus(Resource.OPERATOR_FINALIZER_VALUE)
        }
    }

    fun removeFinalizer(job: V1FlinkJob) {
        val finalizers = job.metadata.finalizers
        if (finalizers != null && finalizers.contains(Resource.OPERATOR_FINALIZER_VALUE)) {
            job.metadata.finalizers = finalizers.minus(Resource.OPERATOR_FINALIZER_VALUE)
        }
    }

    fun hasFinalizer(deployment: V1FlinkDeployment) =
        deployment.metadata.finalizers.orEmpty().contains(Resource.OPERATOR_FINALIZER_VALUE)

    fun addFinalizer(deployment: V1FlinkDeployment) {
        val finalizers = deployment.metadata.finalizers ?: listOf()
        if (!finalizers.contains(Resource.OPERATOR_FINALIZER_VALUE)) {
            deployment.metadata.finalizers = finalizers.plus(Resource.OPERATOR_FINALIZER_VALUE)
        }
    }

    fun removeFinalizer(deployment: V1FlinkDeployment) {
        val finalizers = deployment.metadata.finalizers
        if (finalizers != null && finalizers.contains(Resource.OPERATOR_FINALIZER_VALUE)) {
            deployment.metadata.finalizers = finalizers.minus(Resource.OPERATOR_FINALIZER_VALUE)
        }
    }

    fun isClusterTerminated(cluster: V1FlinkCluster) = FlinkClusterStatus.getSupervisorStatus(cluster) == ClusterStatus.Terminated

    fun hasSupervisorChanged(supervisorDep: V1Deployment, cluster: V1FlinkCluster): Boolean {
        val deployedDigest = supervisorDep.metadata?.annotations?.get("flink-operator/deployment-digest")
        val declaredDigest = Resource.computeDigest(cluster.spec.supervisor)
        return deployedDigest == null || deployedDigest != declaredDigest
    }

    fun createSupervisorDeployment(namespace: String, clusterName: String, cluster: V1FlinkCluster) {
        val deployment = SupervisorResourcesDefaultFactory.createSupervisorDeployment(
            namespace,
            Resource.RESOURCE_OWNER,
            clusterName,
            cluster.spec.supervisor,
            controller.isDryRun()
        )

        controller.createDeployment(namespace, clusterName, deployment)
    }

    fun deleteCluster(namespace: String, clusterName: String) =
        controller.deleteFlinkCluster(namespace, clusterName)

    fun createCluster(namespace: String, clusterName: String, clusterSpec: V1FlinkClusterSpec): Result<Void?> {
        val cluster = DeploymentResourcesDefaultFactory.createFlinkCluster(
            namespace,
            Resource.RESOURCE_OWNER,
            clusterName,
            clusterSpec
        )

        return controller.createFlinkCluster(namespace, clusterName, cluster)
    }

    fun updateCluster(namespace: String, clusterName: String, cluster: V1FlinkCluster): Result<Void?> {
        return controller.updateFlinkCluster(namespace, clusterName, cluster)
    }

    fun deleteJob(namespace: String, clusterName: String, jobName: String) =
        controller.deleteFlinkJob(namespace, clusterName, jobName)

    fun createJob(namespace: String, clusterName: String, jobName: String, jobSpec: V1FlinkJobSpec): Result<Void?> {
        val job = DeploymentResourcesDefaultFactory.createFlinkJob(
            namespace,
            Resource.RESOURCE_OWNER,
            clusterName,
            jobName,
            jobSpec
        )

        return controller.createFlinkJob(namespace, clusterName, jobName, job)
    }

    fun updateJob(namespace: String, clusterName: String, jobName: String, job: V1FlinkJob): Result<Void?> {
        return controller.updateFlinkJob(namespace, clusterName, jobName, job)
    }

    fun deletePod(namespace: String, clusterName: String, name: String): Result<Void?> {
        return controller.deletePod(namespace, clusterName, name)
    }

    fun deleteDeployment(namespace: String, clusterName: String, name: String): Result<Void?> {
        return controller.deleteDeployment(namespace, clusterName, name)
    }

    fun updateFinalizers(namespace: String, clusterName: String, cluster: V1FlinkCluster) {
        controller.updateFinalizers(namespace, clusterName, cluster)
    }

    fun updateStatus(namespace: String, clusterName: String, cluster: V1FlinkCluster) {
        controller.updateStatus(namespace, clusterName, cluster)
    }

    fun updateFinalizers(namespace: String, clusterName: String, jobName: String, job: V1FlinkJob) {
        controller.updateFinalizers(namespace, clusterName, job)
    }

    fun updateStatus(namespace: String, resourceName: String, job: V1FlinkJob) {
        controller.updateStatus(namespace, resourceName, job)
    }

    fun updateFinalizers(namespace: String, clusterName: String, deployment: V1FlinkDeployment) {
        controller.updateFinalizers(namespace, clusterName, deployment)
    }

    fun updateStatus(namespace: String, clusterName: String, deployment: V1FlinkDeployment) {
        controller.updateStatus(namespace, clusterName, deployment)
    }
}